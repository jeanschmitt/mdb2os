package mdb2os

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

func (e *ETL) Sync(ctx context.Context) (bson.Raw, error) {
	e.Logger.Info("Running Sync")

	resumeToken, err := e.obtainResumeToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to generate resume token: %w", err)
	}

	existingIDs, err := e.FindAllIds(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve all existing ids: %w", err)
	}

	mongoIDs, err := e.indexAllDocuments(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to index new documents: %w", err)
	}

	for _, toDelete := range extraItems(mongoIDs, existingIDs) {
		e.EnqueueOperation(DeleteDocument{
			ID: toDelete,
		})
	}

	e.Logger.Info("Sync finished")

	return resumeToken, nil
}

func (e *ETL) obtainResumeToken(ctx context.Context) (bson.Raw, error) {
	stream, err := openChangeStream(ctx, e.Coll, nil)
	if err != nil {
		return nil, err
	}

	defer stream.Close(ctx)

	return stream.ResumeToken(), nil
}

func (e *ETL) indexAllDocuments(ctx context.Context) (map[string]struct{}, error) {
	idSet := make(map[string]struct{})

	cursor, err := e.Coll.Find(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	for cursor.Next(ctx) {
		var doc bson.M
		if err = cursor.Decode(&doc); err != nil {
			return nil, err
		}

		id := fmt.Sprintf("%s", doc["_id"])

		e.EnqueueOperation(IndexDocument{
			ID:       id,
			Document: doc,
		})

		idSet[id] = struct{}{}
	}

	return idSet, nil
}

func extraItems[T comparable](referenceSet map[T]struct{}, slice []T) []T {
	var result []T

	for _, item := range slice {
		if _, ok := referenceSet[item]; !ok {
			result = append(result, item)
		}
	}

	return result
}
