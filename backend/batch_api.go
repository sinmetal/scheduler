package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// BatchAPI is Batch API
type BatchAPI struct{}

func setUpBatch(swPlugin *swagger.Plugin) {
	api := &BatchAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Batch", Description: "Batch list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/batch", hInfo)
	hInfo.Description, hInfo.Tags = "post to batch", []string{tag.Name}
}

// BatchAPIPostRequest is Batch API Post form
type BatchAPIPostRequest struct {
	ProjectID         string `json:"projectID"`
	DstProjectID      string `json:"dstProjectID"`
	DstDatasetID      string `json:"dstDatasetID"`
	DstTableID        string `json:"dstTableID"`
	QueryPathBucket   string `json:"queryPathBucket"`
	QueryPathObject   string `json:"queryPathObject"`
	CreateDisposition string `json:"createDisposition"`
	StartDate         string `json:"startDate"`
	CountDate         int    `json:"countDate"`
}

// BatchAPIPostResponse is Batch API Post response
type BatchAPIPostResponse struct {
}

// Post is Batch API Post Handler
func (api *BatchAPI) Post(ctx context.Context, form *BatchAPIPostRequest) (*BatchAPIPostResponse, error) {
	storageService := storageService{}

	query, err := storageService.GetObject(ctx, form.QueryPathBucket, form.QueryPathObject)
	if err != nil {
		log.Errorf(ctx, "Failed to Get Object From Storage: %v", err)
		return nil, err
	}
	log.Infof(ctx, "query from storage=%s", query)

	var tasks []*taskqueue.Task
	sd, err := time.Parse("20060102", form.StartDate)
	if err != nil {
		return nil, err
	}
	for i := 0; i < form.CountDate; i++ {
		d := sd.AddDate(0, 0, i)
		ds := d.Format("20060102")
		q, err := ExecuteTemplate(query, map[string]interface{}{
			"DATE": ds,
		})
		if err != nil {
			log.Errorf(ctx, "ExecuteTemplate: %v", err)
			return nil, err
		}
		log.Infof(ctx, "query=%s", q)

		body := BigQueryAPIPostRequest{
			ProjectID:    form.ProjectID,
			DstProjectID: form.ProjectID,
			DstDatasetID: form.DstDatasetID,
			DstTableID:   fmt.Sprintf("%s$%s", form.DstTableID, ds),
			Query:        q,
		}
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks,
			&taskqueue.Task{
				Path: "/bigquery",
				Header: http.Header{
					"Content-Type": []string{"application/json"},
				},
				Method:  http.MethodPost,
				Payload: b,
			})
	}

	_, err = taskqueue.AddMulti(ctx, tasks, "batch-query")
	if err != nil {
		log.Errorf(ctx, "taskqueue.AddMulti :%v", err)
		return nil, err
	}

	return &BatchAPIPostResponse{}, nil
}