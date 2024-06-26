package mdb2os

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (e *ETL) Watch(ctx context.Context) error {
	stream, err := openChangeStream(ctx, e.Coll, e.ResumeAfter)
	if err != nil {
		return err
	}

	defer stream.Close(ctx)

	for stream.Next(ctx) {
		var ev ChangeEvent
		if err = stream.Decode(&ev); err != nil {
			return err
		}

		if err = e.processChangeEvent(&ev); err != nil {
			return err
		}
	}

	return stream.Err()
}

func openChangeStream(ctx context.Context, coll *mongo.Collection, resumeAfter any) (*mongo.ChangeStream, error) {
	return coll.Watch(ctx, mongo.Pipeline{
		{{"$match", bson.M{
			"operationType": bson.M{"$in": SupportedOperationTypes},
		}}},
	},
		options.ChangeStream().
			SetFullDocument(options.UpdateLookup).
			SetResumeAfter(resumeAfter),
	)
}

// processChangeEvent mirrors a MongoDB ChangeEvent into OpenSearch.
//
// Returns an error when an unrecoverable error happens.
func (e *ETL) processChangeEvent(changeEvent *ChangeEvent) error {
	switch changeEvent.OperationType {
	case Insert, Update, Replace:
		e.EnqueueOperation(IndexDocument{
			ID:       changeEvent.ID(),
			Document: changeEvent.FullDocument,
		})
	case Delete:
		e.EnqueueOperation(DeleteDocument{
			ID: changeEvent.ID(),
		})
	case Invalidate:
		return ErrInvalidated
	}

	return nil
}

type ChangeEvent struct {
	DocumentKey struct {
		ID any `bson:"_id"`
	} `bson:"documentKey"`
	FullDocument  bson.M        `bson:"fullDocument"`
	OperationType OperationType `bson:"operationType"`
}

func (e *ChangeEvent) ID() string {
	return fmt.Sprintf("%s", e.DocumentKey.ID)
}

type OperationType string

const (
	// Insert occurs when an operation adds documents to a collection.
	Insert OperationType = "insert"
	// Update occurs when an operation updates a document in a collection.
	Update OperationType = "update"
	// Replace occurs when an update operation removes a document from a collection and replaces it with a new document.
	Replace OperationType = "replace"
	// Delete occurs when a document is removed from the collection.
	Delete OperationType = "delete"
	// Invalidate occurs when an operation renders the change stream invalid.
	Invalidate OperationType = "invalidate"
)

var SupportedOperationTypes = []OperationType{
	Insert,
	Update,
	Replace,
	Delete,
	Invalidate,
}

var ErrInvalidated = errors.New("change stream invalidated")
