package main

import (
	"context"
	"crypto/tls"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/opensearch-project/opensearch-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/jeanschmitt/mdb2os"
	"github.com/jeanschmitt/mdb2os/binarycodec"
)

func main() {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		log.Fatal(err.Error())
	}

	osClient, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{cfg.OpenSearch.URL},
		Username:  cfg.OpenSearch.Username,
		Password:  cfg.OpenSearch.Password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: cfg.OpenSearch.SkipVerify},
		},
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	binarycodec.DefaultBinaryDecoder.Register(bson.DefaultRegistry)

	mongoClient, err := mongo.Connect(context.Background(), options.Client().
		ApplyURI(cfg.Mongo.URI),
	)
	if err != nil {
		log.Fatal(err.Error())
	}

	s := mdb2os.NewETL(osClient, mongoClient.Database(cfg.Mongo.Name), mdb2os.Config{
		CollectionName: cfg.Mongo.Collection,
		IndexName:      cfg.OpenSearch.IndexName,
		Logger:         (*LogAdapter)(slog.Default()),
		BulkBatchSize:  100,
		BulkBatchTTL:   time.Millisecond * 50,
	})

	if err = s.Run(context.Background()); err != nil {
		log.Fatal(err.Error())
	}
}

type LogAdapter slog.Logger

func (l *LogAdapter) Info(msg string) { (*slog.Logger)(l).Info(msg) }

func (l *LogAdapter) Warn(msg string) { (*slog.Logger)(l).Warn(msg) }

func (l *LogAdapter) Error(msg string) { (*slog.Logger)(l).Error(msg) }

type Config struct {
	OpenSearch struct {
		URL        string `envconfig:"OPENSEARCH_URL"`
		Username   string `envconfig:"OPENSEARCH_USERNAME"`
		Password   string `envconfig:"OPENSEARCH_PASSWORD"`
		SkipVerify bool   `envconfig:"OPENSEARCH_TLS_SKIP_VERIFY"`
		IndexName  string `envconfig:"OPENSEARCH_INDEX"`
	}
	Mongo struct {
		URI        string `envconfig:"MONGO_URI"`
		Name       string `envconfig:"MONGO_NAME"`
		Collection string `envconfig:"MONGO_COLLECTION"`
	}
}
