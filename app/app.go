package app

import (
	"encoding/json"
	"net/http"
	"os"

	"cloud.google.com/go/bigquery"

	"golang.org/x/net/context"

	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
)

func init() {
	http.HandleFunc("/datasets", handlerDatasets)
	http.HandleFunc("/put", handlerPut)
}

func NewBigQueryClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*bigquery.Client, error) {
	if os.Getenv("BIGQUERY_URI") != "" {
		opts = append(
			opts,
			option.WithEndpoint(os.Getenv("BIGQUERY_URI")),
		)
	}
	return bigquery.NewClient(ctx, projectID, opts...)
}

type DatasetsResponse struct {
	IDs []string `json:"ids"`
}

func handlerDatasets(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	projectID := appengine.AppID(ctx)

	var client *bigquery.Client
	if _client, err := NewBigQueryClient(ctx, projectID); err == nil {
		client = _client
	} else {
		log.Errorf(ctx, "Failed to create bigquery client: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var response DatasetsResponse
	it := client.Datasets(ctx)
	for {
		var dataset *bigquery.Dataset
		if _dataset, err := it.Next(); err == nil {
			dataset = _dataset
		} else if err == iterator.Done {
			break
		} else {
			log.Errorf(ctx, "Failed to iterate over: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		response.IDs = append(response.IDs, dataset.DatasetID)
	}

	var body []byte
	if _body, err := json.Marshal(response); err == nil {
		body = _body
	} else {
		log.Errorf(ctx, "Failed to marshal json: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Add("Content-Type", "application/json")
	w.Write(body)

	return
}

func handlerPut(w http.ResponseWriter, r *http.Request) {
	ctx := appengine.NewContext(r)
	projectID := appengine.AppID(ctx)

	var client *bigquery.Client
	if _client, err := NewBigQueryClient(ctx, projectID); err == nil {
		client = _client
	} else {
		log.Errorf(ctx, "Failed to create bigquery client: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	uploader := client.Dataset("my_dataset").Table("my_table").Uploader()

	type score struct {
		Name string
		Num  int
	}
	scores := []score{
		{Name: "n1", Num: 12},
		{Name: "n2", Num: 31},
		{Name: "n3", Num: 7},
	}

	if err := uploader.Put(ctx, scores); err != nil {
		log.Errorf(ctx, "Failed to put records: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	return
}
