package backend

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"github.com/pkg/errors"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// TQCloudSQLExportAfterAPIPath is Cloud SQL Export後にOPがどうなったかを確認するTaskQueueのPath
const TQCloudSQLExportAfterAPIPath = "/tq/cloudsql/export/after"

// TQCloudSQLExportAfterAPI is Cloud SQL Export後にOPがどうなったかを確認するTask Queue API
type TQCloudSQLExportAfterAPI struct{}

func setupTQCloudSQLExportAfterAPI(swPlugin *swagger.Plugin) {
	api := TQCloudSQLExportAfterAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "TQ Cloud SQL Export After", Description: "TQ Cloud SQL Export After API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, TQCloudSQLExportAfterAPIPath, hInfo)
	hInfo.Description, hInfo.Tags = "post to Cloud SQL Export After", []string{tag.Name}
}

// TQCloudSQLExportAPIAfterPostRequest is Request Payload
type TQCloudSQLExportAPIAfterPostRequest struct {
	ProjectID string `json:"projectId"`
	Instance  string `json:"instance"`
	Operation string `json:"operation"`
	JobKey    string `json:"jobKey"`
}

// Post is Task Queue Handler
func (api *TQCloudSQLExportAfterAPI) Post(ctx context.Context, w http.ResponseWriter, form *TQCloudSQLExportAPIAfterPostRequest) error {
	log.Infof(ctx, "request body = %v", form)
	ds, err := fromContext(ctx)
	if err != nil {
		log.Errorf(ctx, "failed fromContext. %v", err)
		return err
	}

	s := NewCloudSQLAdminService()
	op, err := s.GetOp(ctx, form.ProjectID, form.Operation)
	if err != nil {
		log.Errorf(ctx, "failed CloudSQLAdmin.GetOp, %v", err)
		return err
	}
	log.Infof(ctx, "Operation.Status=%s", op.Status)

	// TODO 失敗して死んだ時のStatusの考慮が必要
	if op.Status == "DONE" {
		jobKey, err := ds.DecodeKey(form.JobKey)
		if err != nil {
			log.Errorf(ctx, "failed Datastore.DecodeKey. %v", err)
			return err
		}
		mustore := CloudSQLExportMutexStore{}
		if err := mustore.Unlock(ctx, form.ProjectID, form.Instance, jobKey); err != nil {
			log.Errorf(ctx, "failed CloudSQLExportMutexStore.Unlock. projectID=%s,Instance=%s,JobKey=%v,err=%v", form.ProjectID, form.Instance, jobKey, err)
			return err
		}
		return nil
	}
	// op 未完了
	w.WriteHeader(http.StatusTeapot)
	return nil
}

// Call is Add to Cloud SQL Export After Task
func (api *TQCloudSQLExportAfterAPI) Call(ctx context.Context, form *TQCloudSQLExportAPIAfterPostRequest) error {
	b, err := json.Marshal(form)
	if err != nil {
		return err
	}

	h := http.Header{}
	h["Content-Type"] = []string{"application/json;charset=utf-8"}
	t := &taskqueue.Task{
		Method:  http.MethodPost,
		Path:    TQCloudSQLExportAfterAPIPath,
		Payload: b,
		Header:  h,
		Delay:   time.Minute * 2,
	}

	tq := NewTaskQueueService()
	_, err = tq.Add(ctx, t, "cloudsql-export-after")
	if err != nil {
		return errors.Wrap(err, "failed taskqueue.add")
	}

	return nil
}
