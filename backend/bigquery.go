package backend

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"

	"google.golang.org/api/bigquery/v2"

	"google.golang.org/appengine/log"

	"golang.org/x/oauth2/google"
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
	client, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		log.Errorf(ctx, "Failed to create client: %v", err)
		return nil, err
	}
	bq, err := bigquery.New(client)
	if err != nil {
		log.Errorf(ctx, "Failed to create bigquery service: %v", err)
		return nil, err
	}

	job, err := bq.Jobs.Insert(form.ProjectID, &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Query: &bigquery.JobConfigurationQuery{
				Query:    form.Query,
				Priority: "Batch",
				DefaultDataset: &bigquery.DatasetReference{
					ProjectId: form.DstProjectID,
					DatasetId: form.DstDatasetID,
				},
				AllowLargeResults: true,
				CreateDisposition: "CreateIfNeeded",
				DestinationTable: &bigquery.TableReference{
					ProjectId: form.DstProjectID,
					DatasetId: form.DstDatasetID,
					TableId:   form.DstTableID,
				},
				TimePartitioning: &bigquery.TimePartitioning{
					Type: "DAY",
				},
				//UseLegacySql:    false,
				//ForceSendFields: []string{"UseLegacySql"},
			},
		},
	}).Do()
	if err != nil {
		log.Errorf(ctx, "Failed to insert query job: %v", err)
		return nil, err
	}

	b, err := json.Marshal(job)
	if err != nil {
		log.Errorf(ctx, "Failed to response job marshal to json: %v", err)
		return nil, err
	}
	log.Infof(ctx, "%s", string(b))

	return &BigQueryAPIPostResponse{
		JobID: job.Id,
	}, nil
}
