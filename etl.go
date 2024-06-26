package mdb2os

import (
	"context"
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
	CollectionName     string
	IndexName          string
	ResumeAfter        bson.Raw
	SyncScrollDuration time.Duration
	SyncBatchSize      int
	Logger             Logger
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
		osChan:     make(chan OpenSearchOperation),
	}
}

func (e *ETL) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error { return e.runOpenSearchWorker(ctx) })

	if e.ResumeAfter == nil {
		var err error
		if e.ResumeAfter, err = e.Sync(ctx); err != nil {
			return err
		}
	}

	eg.Go(func() error { return e.Watch(ctx) })

	return eg.Wait()
}
