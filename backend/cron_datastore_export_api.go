package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"google.golang.org/appengine/log"
)

// CronDatastoreExportAPI is Cron Datastore Export API
type CronDatastoreExportAPI struct{}

func SetupCronDatastoreExportAPI(swPlugin *swagger.Plugin) {
	api := &CronDatastoreExportAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Cron Datastore Export", Description: "Cron Datastore Export API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Get)
	ucon.Handle(http.MethodGet, "/cron/datastore-export", hInfo)
	hInfo.Description, hInfo.Tags = "run to cron datastore export", []string{tag.Name}
}

// Get is Cron Datastore Export API Handler
func (api *CronDatastoreExportAPI) Get(ctx context.Context) error {
	store := ScheduleDatastoreExportStore{}
	l, err := store.ListAll(ctx)
	if err != nil {
		log.Errorf(ctx, "%+v", err)
		return err
	}

	tq := TQDatastoreExportAPI{}
	for _, v := range l {
		// TODO 実行すべきかのハンドリングを追加
		err := tq.Call(ctx, &TQDatastoreExportAPIPostRequest{
			ProjectID: v.ProjectID,
			Bucket:    v.Bucket,
			Kinds:     v.Kinds,
		})
		if err != nil {
			log.Errorf(ctx, "failed %v, %+v", v.Key, err)
			return err
		}
	}

	return nil
}
