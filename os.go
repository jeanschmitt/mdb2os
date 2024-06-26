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

func (e *ETL) IndexDocument(ctx context.Context, id string, document bson.M) error {
	source := OpenSearchSource{
		Document: document,
		Time:     time.Now(),
	}

	jsonDoc, err := json.Marshal(source)
	if err != nil {
		return err
	}

	_, err = e.executeOpenSearchRequest(ctx, opensearchapi.IndexRequest{
		Index:      e.IndexName,
		DocumentID: id,
		Body:       bytes.NewReader(jsonDoc),
	})
	if err != nil {
		return err
	}

	e.Logger.Info(fmt.Sprintf("Indexed document %q: %s", id, string(jsonDoc)))

	return nil
}

type OpenSearchSource struct {
	Document any       `json:"document"`
	Time     time.Time `json:"time"`
}

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
	for {
		select {
		case op := <-e.osChan:
			switch op := op.(type) {
			case IndexDocument:
				if err := e.IndexDocument(ctx, op.ID, op.Document); err != nil {
					// LOG
				}
			case DeleteDocument:
				if err := e.DeleteDocument(ctx, op.ID); err != nil {
					// LOG
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

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
