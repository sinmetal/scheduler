package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
)

// ScheduleDatastoreExportAPI is Datastore ExportのScheduleのためのAPI
type ScheduleDatastoreExportAPI struct{}

func setupScheduleDatastoreExportAPI(swPlugin *swagger.Plugin) {
	api := &ScheduleDatastoreExportAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "ScheduleDatastoreExport", Description: "ScheduleDatastoreExport list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/scheduleDatastoreExport", hInfo)
	hInfo.Description, hInfo.Tags = "post to scheduleDatastoreExport", []string{tag.Name}
}

// ScheduleDatastoreExportAPIPostRequest is ScheduleDatastoreExport API Post form
type ScheduleDatastoreExportAPIPostRequest struct {
	ProjectID string   `json:"projectID"`
	Kinds     []string `json:"kinds"`
	Bucket    string   `json:"bucket"`
}

// ScheduleDatastoreExportAPIPostResponse is ScheduleDatastoreExport API Post response
type ScheduleDatastoreExportAPIPostResponse struct {
	*ScheduleDatastoreExport
}

// Post is ScheduleDatastoreExportAPI API Post Handler
func (api *ScheduleDatastoreExportAPI) Post(ctx context.Context, form *ScheduleDatastoreExportAPIPostRequest) (*ScheduleDatastoreExportAPIPostResponse, error) {
	store := ScheduleDatastoreExportStore{}

	ds, err := fromContext(ctx)
	if err != nil {
		return nil, err
	}

	// TODO Validation
	key := store.NewKey(ctx, ds)
	s := ScheduleDatastoreExport{
		ProjectID: form.ProjectID,
		Kinds:     form.Kinds,
		Bucket:    form.Bucket,
	}
	ss, err := store.Put(ctx, key, &s)
	if err != nil {
		return nil, err
	}
	return &ScheduleDatastoreExportAPIPostResponse{ss}, nil
}
