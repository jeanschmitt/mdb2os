package mdb2os

import (
	"context"
	"fmt"
	"time"

	"github.com/opensearch-project/opensearch-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/mongo"
	"golang.org/x/sync/errgroup"
)

type ETL struct {
	Config
	OpenSearch *opensearch.Client
	Coll       *mongo.Collection

	osChan chan OpenSearchOperation
}

type Config struct {
	// Name of the MongoDB collection to watch.
	CollectionName string

	// Name of the target OpenSearch index.
	IndexName string

	// Match filter use when syncing and watching documents from the MongoDB collection.
	Filter any

	// Filter to use when syncing documents.
	//
	// If set, it overrides Filter.
	//
	// The default value is Filter.
	SyncFilter any

	// Pipeline to use when watching documents.
	//
	// If set, it overrides Filter.
	//
	// The default value is bson.D{{"$match", watchFilter}}, where watchFilter is Filter, each field being prefixed with "fullDocument.".
	WatchPipeline mongo.Pipeline

	// Watch the MongoDB collection after this resume token.
	//
	// If provided, the Sync procedure will be skipped.
	ResumeAfter bson.Raw

	// Duration of the OpenSearch scroll used in the Sync procedure (default: 2 minutes).
	SyncScrollDuration time.Duration

	// Batch size of the OpenSearch scroll used in the Sync procedure (default: 10000).
	SyncBatchSize int

	// If > 1, OpenSearch writes will use the Bulk API, with batches of max BulkBatchSize operations.
	BulkBatchSize int

	// If > 0, Bulk write batches will be sent after BulkBatchTTL, even if they are not full.
	BulkBatchTTL time.Duration

	// BSON registry to use when transforming a MongoDB document to an OpenSearch document (default: Mongo client registry).
	TransformRegistry *bsoncodec.Registry

	// Logger to use (default: a NoopLogger).
	Logger Logger
}

func (cfg *Config) SetDefaults() error {
	if cfg.SyncScrollDuration == 0 {
		cfg.SyncScrollDuration = 2 * time.Minute
	}
	if cfg.SyncBatchSize == 0 {
		cfg.SyncBatchSize = 10000
	}
	if cfg.Logger == nil {
		cfg.Logger = NoopLogger{}
	}
	if cfg.SyncFilter == nil {
		cfg.SyncFilter = cfg.Filter
	}
	if cfg.WatchPipeline == nil && cfg.Filter != nil {
		match, err := prefixBsonDocument(cfg.Filter, "fullDocument.")
		if err != nil {
			return fmt.Errorf("invalid filter: %w", err)
		}

		cfg.WatchPipeline = mongo.Pipeline{{{
			"$match", match,
		}}}
	}

	return nil
}

func NewETL(openSearchClient *opensearch.Client, mongoDB *mongo.Database, cfg Config) (*ETL, error) {
	if err := cfg.SetDefaults(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &ETL{
		Config:     cfg,
		OpenSearch: openSearchClient,
		Coll:       mongoDB.Collection(cfg.CollectionName),
		osChan:     make(chan OpenSearchOperation, cfg.BulkBatchSize),
	}, nil
}

func (e *ETL) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error { return e.runOpenSearchWorker(ctx) })

	if e.ResumeAfter == nil {
		var err error
		if e.ResumeAfter, err = e.Sync(ctx); err != nil {
			return fmt.Errorf("failed to sync: %w", err)
		}
	}

	eg.Go(func() error { return e.Watch(ctx) })

	return eg.Wait()
}

func prefixBsonDocument(doc any, prefix string) (bson.D, error) {
	bsonEncoded, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}

	var d bson.D
	if err = bson.Unmarshal(bsonEncoded, &d); err != nil {
		return nil, err
	}

	for i, field := range d {
		d[i].Key = prefix + field.Key
	}

	return d, nil
}
