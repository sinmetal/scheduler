package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
)

// ScheduleAPI is Schedule API
type ScheduleAPI struct{}

func SetupScheduleAPI(swPlugin *swagger.Plugin) {
	api := &ScheduleAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Schedule", Description: "Schedule list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/schedule", hInfo)
	hInfo.Description, hInfo.Tags = "post to schedule", []string{tag.Name}
}

// ScheduleAPIPostRequest is Schedule API Post form
type ScheduleAPIPostRequest struct {
	ProjectID         string `json:"projectID"`
	DstProjectID      string `json:"dstProjectID"`
	DstDatasetID      string `json:"dstDatasetID"`
	DstTableID        string `json:"dstTableID"`
	QueryPathBucket   string `json:"queryPathBucket"`
	QueryPathObject   string `json:"queryPathObject"`
	CreateDisposition string `json:"createDisposition"`
	ScheduleV1        string `json:"scheduleV1"`
	ScheduleTimezone  string `json:"scheduleTimezone"`
}

// ScheduleAPIPostResponse is Schedule API Post response
type ScheduleAPIPostResponse struct {
	*Schedule
}

// Post is Schedule API Post Handler
func (api *ScheduleAPI) Post(ctx context.Context, form *ScheduleAPIPostRequest) (*ScheduleAPIPostResponse, error) {
	store := ScheduleStore{}

	ds, err := fromContext(ctx)
	if err != nil {
		return nil, err
	}

	// TODO Validation
	key := store.NewKey(ctx, ds)
	s := Schedule{
		ProjectID:       form.ProjectID,
		DstProjectID:    form.DstProjectID,
		DstDatasetID:    form.DstDatasetID,
		DstTableID:      form.DstTableID,
		QueryPathBucket: form.QueryPathBucket,
		QueryPathObject: form.QueryPathObject,
		ScheduleV1:      form.ScheduleV1,
	}
	ss, err := store.Put(ctx, key, &s)
	if err != nil {
		return nil, err
	}
	return &ScheduleAPIPostResponse{ss}, nil
}
