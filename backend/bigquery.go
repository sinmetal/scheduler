package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"

	"cloud.google.com/go/bigquery"

	"google.golang.org/appengine/log"
)

// BigQueryAPI is BigQuery API
type BigQueryAPI struct{}

func setupBigQuery(swPlugin *swagger.Plugin) {
	api := &BigQueryAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "BigQuery", Description: "BigQuery list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/bigquery", hInfo)
	hInfo.Description, hInfo.Tags = "post to bigquery", []string{tag.Name}
}

// BigQueryAPIPostRequest is BigQuery API Post form
type BigQueryAPIPostRequest struct {
	ProjectID         string `json:"projectID"`
	DstProjectID      string `json:"dstProjectID"`
	DstDatasetID      string `json:"dstDatasetID"`
	DstTableID        string `json:"dstTableID"`
	Query             string `json:"query"`
	CreateDisposition string `json:"createDisposition"`
}

// BigQueryAPIPostResponse is BigQuery API Post response
type BigQueryAPIPostResponse struct {
	JobID string `json:"jobID"`
}

// Post is BigQuery API Post Handler
func (api *BigQueryAPI) Post(ctx context.Context, form *BigQueryAPIPostRequest) (*BigQueryAPIPostResponse, error) {
	client, err := bigquery.NewClient(ctx, form.ProjectID)
	if err != nil {
		log.Errorf(ctx, "Failed to create client: %v", err)
		return nil, err
	}

	q := client.Query(form.Query)
	q.Priority = bigquery.BatchPriority
	q.DefaultProjectID = form.ProjectID
	q.DefaultDatasetID = form.DstDatasetID
	q.AllowLargeResults = true
	q.CreateDisposition = bigquery.CreateIfNeeded // 選択可能な必要がある
	q.Dst = &bigquery.Table{
		ProjectID: form.DstProjectID,
		DatasetID: form.DstDatasetID,
		TableID:   form.DstTableID,
	}
	q.UseLegacySQL = false
	job, err := q.Run(ctx)
	if err != nil {
		log.Errorf(ctx, "Failed to insert query job: %v", err)
		return nil, err
	}
	return &BigQueryAPIPostResponse{
		JobID: job.ID(),
	}, nil
}
