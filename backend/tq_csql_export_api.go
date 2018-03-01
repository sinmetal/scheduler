package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"github.com/pkg/errors"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// TQCloudSQLExportAPIPath is Cloud SQL ExportのTaskQueue用のPath
const TQCloudSQLExportAPIPath = "/tq/cloudsql/export"

// TQCloudSQLExportAPI is Cloud SQL ExportのAPIを呼び出すTask Queue API
type TQCloudSQLExportAPI struct{}

func setupTQCloudSQLExportAPI(swPlugin *swagger.Plugin) {
	api := TQCloudSQLExportAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "TQ Cloud SQL Export", Description: "TQ Cloud SQL Export API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, TQCloudSQLExportAPIPath, hInfo)
	hInfo.Description, hInfo.Tags = "post to Cloud SQL Export", []string{tag.Name}
}

// TQCloudSQLExportAPIPostRequest is
type TQCloudSQLExportAPIPostRequest struct {
	ProjectID string   `json:"projectID"` // ExportするCloudSQLが存在するProjectID
	Instance  string   `json:"instance"`  // ExportするCloudSQLのInstanceID
	Databases []string `json:"databases"` // ExportするCloudSQLのDatabase
	SQLBucket string   `json:"sqlBucket"` // Export時に利用するSQLを置いているGCS Bucket. hoge
	SQLObject string   `json:"sqlObject"` // Export時に利用するSQLを置いているGCS Object. fuga.csv
	ExportURI string   `json:"exportURI"` // Export先のGCS Path. %sを入れるとyyyyMMddhhmmに置き換える gs://hoge/%s/fuga.csv
}

// Post is Task Queue Handler
func (api *TQCloudSQLExportAPI) Post(ctx context.Context, form *TQCloudSQLExportAPIPostRequest) error {
	log.Infof(ctx, "request body = %v", form)

	storageService := storageService{}
	query, err := storageService.GetObject(ctx, form.SQLBucket, form.SQLObject)
	if err != nil {
		log.Errorf(ctx, "Failed to Get Object From Storage:\n %+v", err)
		return err
	}
	log.Infof(ctx, "query from storage=%s", query)

	const dateLayout = "20060102150405"
	y := time.Now().Format(dateLayout)
	s := CloudSQLAdminService{}
	err = s.Export(ctx, &CloudSQLExportConfig{
		ProjectID: form.ProjectID,
		Instance:  form.Instance,
		Databases: form.Databases,
		SQL:       query,
		ExportURI: fmt.Sprintf(form.ExportURI, y),
	})
	if err != nil {
		log.Errorf(ctx, "Failed to Cloud SQL Export:\n %+v", err)
		return err
	}

	return nil
}

// Call is Add to Cloud SQL Export Task
func (api *TQCloudSQLExportAPI) Call(ctx context.Context, form *TQCloudSQLExportAPIPostRequest) error {
	b, err := json.Marshal(form)
	if err != nil {
		return err
	}

	h := http.Header{}
	h["Content-Type"] = []string{"application/json;charset=utf-8"}
	t := &taskqueue.Task{
		Method:  http.MethodPost,
		Path:    TQCloudSQLExportAPIPath,
		Payload: b,
		Header:  h,
	}

	tq := NewTaskQueueService()
	_, err = tq.Add(ctx, t, "cloudsql-export")
	if err != nil {
		return errors.Wrap(err, "failed taskqueue.add")
	}

	return nil
}
