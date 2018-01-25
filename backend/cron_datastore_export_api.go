package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	//"google.golang.org/appengine/log"
)

type CronDatastoreExportAPI struct{}

func setupCronDatastoreExportAPI(swPlugin *swagger.Plugin) {
	api := &CronDatastoreExportAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Cron Datastore Export", Description: "Cron Datastore Export API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Get)
	ucon.Handle(http.MethodGet, "/cron/query", hInfo)
	hInfo.Description, hInfo.Tags = "run to cron query", []string{tag.Name}
}

func (api *CronDatastoreExportAPI) Get(ctx context.Context) error {
	//store := ScheduleDatastoreExportStore{}
	//l, err := store.ListAll(ctx)
	//if err != nil {
	//	log.Errorf(ctx, "%+v", err)
	//	return err
	//}

	//for i, v := range l {
	//	// TODO 実行すべきかのハンドリングを追加
	//
	//
	//}

	return nil
}
