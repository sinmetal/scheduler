package backend

import (
	"context"
	"fmt"
	"net/http"

	"ds2bq"
	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"google.golang.org/appengine/log"
)

// TQDatastoreExportAPI is Datastore Export Job Start API
type TQDatastoreExportAPI struct{}

func setupTQDatastoreExportAPI(swPlugin *swagger.Plugin) {
	api := TQDatastoreExportAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "TQ Datastore Export", Description: "TQ Datastore Export API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/tq/datastore/export", hInfo)
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

	s := ds2bq.NewDS2BQService()
	err := s.Export(ctx, &ds2bq.ExportForm{
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
