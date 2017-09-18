package app

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/labstack/echo"

	api_bigquery_v2 "google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine/aetest"
)

func PrepareDummyBigquery() func() {
	dummyHandler := echo.New()
	dummyHandler.HTTPErrorHandler = func(err error, c echo.Context) {
		log.Printf("ERROR: %v: %v: %v", c.Request().Method, c.Request().URL, err)
		c.NoContent(http.StatusNotFound)
	}
	dummyBigquery := dummyHandler.Group("/api/bigquery/v2")
	dummyBigquery.GET("/projects/:projectID/datasets", func(c echo.Context) error {
		projectID := c.Param("projectID")
		return c.JSON(http.StatusOK, &api_bigquery_v2.DatasetList{
			Datasets: []*api_bigquery_v2.DatasetListDatasets{
				&api_bigquery_v2.DatasetListDatasets{
					DatasetReference: &api_bigquery_v2.DatasetReference{
						DatasetId: "test1",
						ProjectId: projectID,
					},
					FriendlyName: "Dummy dataset for testing #1",
					Id:           "test1",
					Kind:         "bigquery#dataset",
					Labels: map[string]string{
						"purpose": "test",
					},
				},
				&api_bigquery_v2.DatasetListDatasets{
					DatasetReference: &api_bigquery_v2.DatasetReference{
						DatasetId: "test2",
						ProjectId: projectID,
					},
					FriendlyName: "Dummy dataset for testing #2",
					Id:           "test1",
					Kind:         "bigquery#dataset",
					Labels: map[string]string{
						"purpose": "test",
					},
				},
			},
			Etag:          "xxxx",
			Kind:          "bigquery#datasetList",
			NextPageToken: "",
		})
	})
	dummyBigquery.POST(
		"/projects/:projectID/datasets/:datasetID/tables/:tableID/insertAll",
		func(c echo.Context) error {
			return c.JSON(http.StatusOK, &api_bigquery_v2.TableDataInsertAllResponse {
				InsertErrors: []*api_bigquery_v2.TableDataInsertAllResponseInsertErrors{},
				Kind: "bigquery#tableDataInsertAllResponse",
			})
		},
	)
	dummyBigqueryServer := httptest.NewServer(dummyHandler)

	if err := os.Setenv(
		"BIGQUERY_URI",
		fmt.Sprintf("%s/api/bigquery/v2/", dummyBigqueryServer.URL),
	); err != nil {
		dummyBigqueryServer.Close()
		panic(err)
	}

	return func(){
		os.Setenv("BIGQUERY_URI", "")
		dummyBigqueryServer.Close()
	}
}

func TestHandlerDatasets(t *testing.T) {
	teardown := PrepareDummyBigquery()
	defer teardown()

	var inst aetest.Instance
	if _inst, err := aetest.NewInstance(&aetest.Options{
		StronglyConsistentDatastore: true,
	}); err == nil {
		inst = _inst
	} else {
		panic(err)
	}
	defer inst.Close()

	var req *http.Request
	if _req, err := inst.NewRequest("GET", "/datasets", nil); err == nil {
		req = _req
	} else {
		panic(err)
	}
	res := httptest.NewRecorder()

	handlerDatasets(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Expected 200, but %v", res.Code)
	}

	body := res.Body.Bytes()
	var data DatasetsResponse
	if err := json.Unmarshal(body, &data); err != nil {
		t.Fatalf("Unexpected data: %v", string(body))
	}

	sort.Strings(data.IDs)
	if !reflect.DeepEqual([]string{"test1", "test2"}, data.IDs) {
		t.Errorf("Expected {test1, test2}, but was %#v", data)
	}
}

func TestHandlerPut(t *testing.T) {
	teardown := PrepareDummyBigquery()
	defer teardown()

	var inst aetest.Instance
	if _inst, err := aetest.NewInstance(&aetest.Options{
		StronglyConsistentDatastore: true,
	}); err == nil {
		inst = _inst
	} else {
		panic(err)
	}
	defer inst.Close()

	var req *http.Request
	if _req, err := inst.NewRequest("GET", "/put", nil); err == nil {
		req = _req
	} else {
		panic(err)
	}
	res := httptest.NewRecorder()

	handlerPut(res, req)

	if res.Code != http.StatusOK {
		t.Fatalf("Expected 200, but %v", res.Code)
	}
}
