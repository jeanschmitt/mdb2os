package mdb2os

import (
	"context"
	"fmt"
	"time"

	"github.com/opensearch-project/opensearch-go"
	"go.mongodb.org/mongo-driver/bson"
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

	// Logger to use (default: a NoopLogger).
	Logger Logger
}

func (cfg *Config) SetDefaults() {
	if cfg.SyncScrollDuration == 0 {
		cfg.SyncScrollDuration = 2 * time.Minute
	}
	if cfg.SyncBatchSize == 0 {
		cfg.SyncBatchSize = 10000
	}
	if cfg.Logger == nil {
		cfg.Logger = NoopLogger{}
	}
}

func NewETL(
	openSearchClient *opensearch.Client,
	mongoDB *mongo.Database,
	cfg Config,
) *ETL {
	cfg.SetDefaults()

	return &ETL{
		Config:     cfg,
		OpenSearch: openSearchClient,
		Coll:       mongoDB.Collection(cfg.CollectionName),
		osChan:     make(chan OpenSearchOperation, cfg.BulkBatchSize),
	}
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
