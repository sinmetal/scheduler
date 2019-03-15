package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"google.golang.org/appengine/log"
)

// CronCloudSQLExportAPI is Cron Cloud SQL Export API
type CronCloudSQLExportAPI struct{}

func SetupCronCloudSQLExportAPI(swPlugin *swagger.Plugin) {
	api := &CronCloudSQLExportAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Cron Cloud SQL Export", Description: "Cron Cloud SQL Export API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Get)
	ucon.Handle(http.MethodGet, "/cron/cloudsql-export", hInfo)
	hInfo.Description, hInfo.Tags = "run to cron cloud sql export", []string{tag.Name}
}

// Get is Cron Datastore Export API Handler
func (api *CronCloudSQLExportAPI) Get(ctx context.Context) error {
	store := ScheduleCloudSQLExportStore{}
	l, err := store.ListAll(ctx)
	if err != nil {
		log.Errorf(ctx, "%+v", err)
		return err
	}

	tq := TQCloudSQLExportAPI{}
	for _, v := range l {
		// TODO 実行すべきかのハンドリングを追加
		err := tq.Call(ctx, &TQCloudSQLExportAPIPostRequest{
			ProjectID:           v.ProjectID,
			Instance:            v.Instance,
			Databases:           v.Databases,
			SQLBucket:           v.SQLBucket,
			SQLObject:           v.SQLObject,
			ExportURI:           v.ExportURI,
			BigQueryProjectID:   v.BigQueryProjectID,
			BigQueryDataset:     v.BigQueryDataset,
			BigQueryTable:       v.BigQueryTable,
			BigQueryTableSchema: v.BigQueryTableSchema,
		})
		if err != nil {
			log.Errorf(ctx, "failed %v, %+v", v.Key, err)
			return err
		}
	}

	return nil
}
