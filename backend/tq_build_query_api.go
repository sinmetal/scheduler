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

// TQBuildQueryAPI is Build Query API
type TQBuildQueryAPI struct{}

func SetupTQBuildQueryAPI(swPlugin *swagger.Plugin) {
	api := TQBuildQueryAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "TQ Build Query", Description: "TQ Build Query list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/tq/buildQuery", hInfo)
	hInfo.Description, hInfo.Tags = "post to Build Query", []string{tag.Name}
}

// TQBuildQueryAPIPostRequest is Build Query API Post form
type TQBuildQueryAPIPostRequest struct {
	ProjectID         string    `json:"projectID"`
	DstProjectID      string    `json:"dstProjectID"`
	DstDatasetID      string    `json:"dstDatasetID"`
	DstTableID        string    `json:"dstTableID"`
	QueryPathBucket   string    `json:"queryPathBucket"`
	QueryPathObject   string    `json:"queryPathObject"`
	CreateDisposition string    `json:"createDisposition"`
	TargetDate        time.Time `json:"targetDate"`
}

// TQBuildQueryAPIPostResponse is Build Query API Post response
type TQBuildQueryAPIPostResponse struct {
}

// Post is TQBuildQueryAPI Post Handler
func (api *TQBuildQueryAPI) Post(ctx context.Context, form *TQBuildQueryAPIPostRequest) (*TQBuildQueryAPIPostResponse, error) {
	storageService := NewStorageService()

	query, err := storageService.GetObject(ctx, form.QueryPathBucket, form.QueryPathObject)
	if err != nil {
		log.Errorf(ctx, "Failed to Get Object From Storage:\n %+v", err)
		return nil, err
	}
	log.Infof(ctx, "query from storage=%s", query)

	const dateLayout = "20060102"
	const queueName = "schedule-query"
	d := form.TargetDate.Format(dateLayout)
	q, err := ExecuteTemplate(query, map[string]interface{}{
		"DATE": d,
	})
	if err != nil {
		log.Errorf(ctx, "ExecuteTemplate: %+v", err)
		return nil, err
	}
	log.Infof(ctx, "query=%s", q)

	body := BigQueryAPIPostRequest{
		ProjectID:    form.ProjectID,
		DstProjectID: form.ProjectID,
		DstDatasetID: form.DstDatasetID,
		DstTableID:   fmt.Sprintf("%s$%s", form.DstTableID, d),
		Query:        q,
	}
	b, err := json.Marshal(body)
	if err != nil {
		log.Errorf(ctx, "json.Marshal: %+v", err)
		return nil, err
	}

	_, err = taskqueue.Add(ctx, &taskqueue.Task{
		Path: "/bigquery",
		Header: http.Header{
			"Content-Type": []string{"application/json"},
		},
		Method:  http.MethodPost,
		Payload: b,
	}, queueName)
	if err != nil {
		log.Errorf(ctx, "taskqueue.Add: %+v", err)
		return nil, err
	}

	return &TQBuildQueryAPIPostResponse{}, nil
}
