package mdb2os

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"go.mongodb.org/mongo-driver/bson"
)

// IndexDocument indexes a single OpenSearch document.
func (e *ETL) IndexDocument(ctx context.Context, id string, document bson.M) error {
	source, err := newSource(document)
	if err != nil {
		return err
	}

	_, err = e.executeOpenSearchRequest(ctx, opensearchapi.IndexRequest{
		Index:      e.IndexName,
		DocumentID: id,
		Body:       bytes.NewReader(source),
	})
	if err != nil {
		return err
	}

	e.Logger.Info(fmt.Sprintf("Indexed document %q", id))

	return nil
}

// DeleteDocument deletes a single OpenSearch document.
func (e *ETL) DeleteDocument(ctx context.Context, id string) error {
	_, err := e.executeOpenSearchRequest(ctx, opensearchapi.DeleteRequest{
		Index:      e.IndexName,
		DocumentID: id,
	})
	if err != nil {
		return err
	}

	e.Logger.Info(fmt.Sprintf("Deleted document %q", id))

	return nil
}

// BulkWrite sends a Bulk operation to OpenSearch, with the given operations batch.
func (e *ETL) BulkWrite(ctx context.Context, batch []OpenSearchOperation) error {
	body, err := newBulkBody(batch)
	if err != nil {
		return fmt.Errorf("failed to build bulk body: %w", err)
	}

	bulkRes, err := e.executeOpenSearchRequest(ctx, opensearchapi.BulkRequest{
		Index: e.IndexName,
		Body:  body,
	})
	if err != nil {
		return fmt.Errorf("failed send bulk request: %w", err)
	}

	if _, isNoopLogger := e.Logger.(NoopLogger); !isNoopLogger {
		var parsedRes BulkResponse
		err = json.Unmarshal(bulkRes, &parsedRes)
		if err != nil {
			return fmt.Errorf("could not parse bulk response: %w", err)
		}

		for _, op := range parsedRes.Items {
			if op.Index != nil {
				if op.Index.Error != nil {
					e.Logger.Error(fmt.Sprintf("Failed to index document %s: %v", op.Index.ID, op.Index.Error))
				} else {
					e.Logger.Info(fmt.Sprintf("Indexed document %s", op.Index.ID))
				}
			}
			if op.Delete != nil {
				if op.Delete.Error != nil {
					e.Logger.Error(fmt.Sprintf("Failed to delete document %s: %v", op.Delete.ID, op.Delete.Error))
				} else {
					e.Logger.Info(fmt.Sprintf("Deleted document %s", op.Delete.ID))
				}
			}
		}
	}

	return nil
}

func newBulkBody(batch []OpenSearchOperation) (*bytes.Buffer, error) {
	body := new(bytes.Buffer)

	for _, op := range batch {
		switch op := op.(type) {
		case IndexDocument:
			source, err := newSource(op.Document)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal document %s: %w", op.ID, err)
			}

			body.WriteString(fmt.Sprintf("{\"index\":{\"_id\":%q}}\n", op.ID))
			body.Write(source)

		case DeleteDocument:
			body.WriteString(fmt.Sprintf("{\"delete\":{\"_id\":%q}}", op.ID))
		}

		body.WriteByte('\n')
	}

	return body, nil
}

func newSource(document map[string]any) ([]byte, error) {
	source := OpenSearchSource{
		Document: document,
		Time:     time.Now(),
	}

	return json.Marshal(source)
}

type BulkResponse struct {
	Items []struct {
		Index  *BulkOperationResult `json:"index"`
		Delete *BulkOperationResult `json:"delete"`
	} `json:"items"`
}

type BulkOperationResult struct {
	ID    string              `json:"_id"`
	Error *BulkOperationError `json:"error"`
}

type BulkOperationError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

type OpenSearchSource struct {
	Document any       `json:"document"`
	Time     time.Time `json:"time"`
}

func (e *ETL) FindAllIds(ctx context.Context) ([]string, error) {
	// Initial search request
	res, err := e.executeOpenSearchRequest(ctx, opensearchapi.SearchRequest{
		Body:   strings.NewReader(`{"_source":false}`),
		Index:  []string{e.IndexName},
		Scroll: e.SyncScrollDuration,
		Size:   &e.SyncBatchSize,
		Sort:   []string{"_doc"}, // disable sorting, optimized for scan the whole index
	})
	if err != nil {
		var osError *OpenSearchError
		if errors.As(err, &osError) && osError.StatusCode == 404 {
			return nil, nil
		}
		return nil, err
	}

	var scanRes scanIDsResponse
	if err = json.Unmarshal(res, &scanRes); err != nil {
		return nil, err
	}

	defer e.clearScroll(context.Background(), scanRes.ScrollID)

	var ids []string

	for len(scanRes.Hits.Hits) > 0 {
		for _, id := range scanRes.Hits.Hits {
			ids = append(ids, id.ID)
		}

		res, err = e.executeOpenSearchRequest(ctx, opensearchapi.ScrollRequest{
			ScrollID: scanRes.ScrollID,
			Scroll:   e.SyncScrollDuration,
		})
		if err != nil {
			return nil, err
		}

		if err = json.Unmarshal(res, &scanRes); err != nil {
			return nil, err
		}
	}

	return ids, nil
}

type scanIDsResponse struct {
	ScrollID string `json:"_scroll_id"`
	Hits     struct {
		Hits []struct {
			ID string `json:"_id"`
		} `json:"hits"`
	} `json:"hits"`
}

func (e *ETL) clearScroll(ctx context.Context, scrollID string) error {
	_, err := e.executeOpenSearchRequest(ctx, opensearchapi.ClearScrollRequest{
		ScrollID: []string{scrollID},
	})
	return err
}

func (e *ETL) executeOpenSearchRequest(ctx context.Context, req opensearchapi.Request) (body []byte, err error) {
	res, err := req.Do(ctx, e.OpenSearch)
	if err != nil {
		return nil, fmt.Errorf("error getting response: %w", err)
	}
	defer res.Body.Close()

	return handleOpenSearchResponse(res)
}

func handleOpenSearchResponse(res *opensearchapi.Response) (body []byte, err error) {
	if res.Body != nil {
		body, _ = io.ReadAll(res.Body)
	}

	if res.HasWarnings() {
		log.Println("OpenSearch returned warnings:", res.Warnings())
	}

	if res.IsError() {
		return nil, &OpenSearchError{StatusCode: res.StatusCode, Body: body}
	}

	return body, nil
}

func (e *ETL) EnqueueOperation(op OpenSearchOperation) {
	e.osChan <- op
}

func (e *ETL) runOpenSearchWorker(ctx context.Context) error {
	if e.BulkBatchSize <= 1 {
		return e.openSearchWorkerNoBatch(ctx)
	}

	if e.BulkBatchSize <= 0 {
		return e.openSearchWorkerNoTTL(ctx)
	}

	return e.batchedOpenSearchWorker(ctx)
}

func (e *ETL) openSearchWorkerNoBatch(ctx context.Context) error {
	for {
		select {
		case op := <-e.osChan:
			switch op := op.(type) {
			case IndexDocument:
				if err := e.IndexDocument(ctx, op.ID, op.Document); err != nil {
					e.Logger.Error(fmt.Sprintf("Failed to index document %s: %v", op.ID, err))
				}
			case DeleteDocument:
				if err := e.DeleteDocument(ctx, op.ID); err != nil {
					e.Logger.Error(fmt.Sprintf("Failed to delete document %s: %v", op.ID, err))
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (e *ETL) openSearchWorkerNoTTL(ctx context.Context) error {
	for {
		batch := make([]OpenSearchOperation, 0, e.BulkBatchSize)

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case op, ok := <-e.osChan:
				if !ok {
					return ErrChanClosed
				}

				batch = append(batch, op)
				if len(batch) == e.BulkBatchSize {
					if err := e.BulkWrite(ctx, batch); err != nil {
						e.Logger.Error(fmt.Sprintf("Failed to bulk write: %v", err))
					}
				}
			}
		}
	}
}

func (e *ETL) batchedOpenSearchWorker(ctx context.Context) error {
	for {
		batch := make([]OpenSearchOperation, 0, e.BulkBatchSize)
		after := time.After(e.BulkBatchTTL)

	batchLoop:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case op, ok := <-e.osChan:
				if !ok {
					return ErrChanClosed
				}

				batch = append(batch, op)
				if len(batch) == e.BulkBatchSize {
					break batchLoop
				}

			case <-after:
				break batchLoop
			}
		}

		if len(batch) > 0 {
			if err := e.BulkWrite(ctx, batch); err != nil {
				e.Logger.Error(fmt.Sprintf("Failed to bulk write: %v", err))
			}
		}
	}
}

var ErrChanClosed = errors.New("channel closed")

type OpenSearchOperation interface {
	IsOpenSearchOperation()
}

type IndexDocument struct {
	ID       string
	Document map[string]any
}

type DeleteDocument struct {
	ID string
}

func (IndexDocument) IsOpenSearchOperation()  {}
func (DeleteDocument) IsOpenSearchOperation() {}

type OpenSearchError struct {
	StatusCode int
	Body       []byte
}

func (e *OpenSearchError) Error() string {
	if len(e.Body) == 0 {
		return fmt.Sprintf("got %d error from opensearch", e.StatusCode)
	}
	return fmt.Sprintf("got %d error from opensearch: %s", e.StatusCode, e.Body)
}
