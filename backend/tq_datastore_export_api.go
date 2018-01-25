package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"github.com/pkg/errors"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// TQDatastoreExportAPI is Datastore Export Job Start API
type TQDatastoreExportAPI struct {
	Path string
}

func setupTQDatastoreExportAPI(swPlugin *swagger.Plugin) {
	api := TQDatastoreExportAPI{
		Path: "/tq/datastore/export",
	}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "TQ Datastore Export", Description: "TQ Datastore Export API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, api.Path, hInfo)
	hInfo.Description, hInfo.Tags = "post to Datastore Export", []string{tag.Name}
}

// TQDatastoreExportAPIPostRequest is Datastore Export Job Start API Post form
type TQDatastoreExportAPIPostRequest struct {
	ProjectID string   `json:"projectID"`
	Bucket    string   `json:"bucket"`
	Kinds     []string `json:"kinds"`
}

// Post is Datastore Export Job Start API Handler
func (api *TQDatastoreExportAPI) Post(ctx context.Context, form *TQDatastoreExportAPIPostRequest) error {
	log.Infof(ctx, "request body = %v", form)

	s := NewDS2BQService()
	err := s.Export(ctx, &DatastoreExportForm{
		ProjectID: form.ProjectID,
		Bucket:    fmt.Sprintf("gs://%s", form.Bucket),
		Kinds:     form.Kinds,
	})
	if err != nil {
		log.Errorf(ctx, "failed datastore export : %v", err)
		return err
	}

	return nil
}

// Call is Add to Task
func (api *TQDatastoreExportAPI) Call(ctx context.Context, form *TQDatastoreExportAPIPostRequest) error {
	b, err := json.Marshal(form)
	if err != nil {
		return err
	}

	h := http.Header{}
	h["Content-Type"] = []string{"application/json;charset=utf-8"}
	t := &taskqueue.Task{
		Method:  http.MethodPost,
		Path:    api.Path,
		Payload: b,
		Header:  h,
	}
	_, err = taskqueue.Add(ctx, t, "datastore-export")
	if err != nil {
		return errors.Wrap(err, "failed taskqueue.add")
	}

	return nil
}
