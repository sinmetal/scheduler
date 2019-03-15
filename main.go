package main

import (
	"net/http"
	"reflect"
	"strings"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"github.com/sinmetal/scheduler/backend"
	"google.golang.org/appengine"
)

func main() {
	ucon.Middleware(UseAppengineContext)
	ucon.Orthodox()
	ucon.Middleware(swagger.RequestValidator())

	swPlugin := swagger.NewPlugin(&swagger.Options{
		Object: &swagger.Object{
			Info: &swagger.Info{
				Title:   "scheduler",
				Version: "v1",
			},
			Schemes: []string{"http", "https"},
		},
		DefinitionNameModifier: func(refT reflect.Type, defName string) string {
			if strings.HasSuffix(defName, "JSON") {
				return defName[:len(defName)-4]
			}
			return defName
		},
	})
	ucon.Plugin(swPlugin)

	backend.SetupBigQuery(swPlugin)
	backend.SetupBatch(swPlugin)
	backend.SetupScheduleAPI(swPlugin)
	backend.SetupScheduleDatastoreExportAPI(swPlugin)
	backend.SetupScheduleCloudSQLExportAPI(swPlugin)
	backend.SetupStorageBQLoadConfigAPI(swPlugin)
	backend.SetupTQBuildQueryAPI(swPlugin)
	backend.SetupTQDatastoreExportAPI(swPlugin)
	backend.SetupTQCloudSQLExportAPI(swPlugin)
	backend.SetupTQCloudSQLExportAfterAPI(swPlugin)
	backend.SetupCronQueryAPI(swPlugin)
	backend.SetupCronDatastoreExportAPI(swPlugin)
	backend.SetupCronCloudSQLExportAPI(swPlugin)

	ucon.HandleFunc(http.MethodPost, "/ocn/datastore-export", backend.ReceiveOCNHandler)
	ucon.HandleFunc(http.MethodPost, "/tq/gcs/object-to-bq", backend.ImportBigQueryHandleFunc("datastore_imports"))
	ucon.HandleFunc(http.MethodPost, "/ocn/cloudsql-export", backend.ReceiveCloudSQLExportOCNHandler)
	ucon.HandleFunc(http.MethodPost, "/ocn/storage-bqload", backend.ReceiveStorageBQLoadOCNHandler)
	ucon.HandleFunc(http.MethodPost, "/_ah/push-handlers/sample", backend.ReceivePubSubSampleHandler)
	ucon.HandleFunc(http.MethodPost, "/_ah/push-handlers/cloudsql-export", backend.ReceiveCloudSQLExportPubSubHandler)

	ucon.DefaultMux.Prepare()
	http.Handle("/", ucon.DefaultMux)

	appengine.Main()
}

// UseAppengineContext is UseAppengineContext
func UseAppengineContext(b *ucon.Bubble) error {
	if b.Context == nil {
		b.Context = appengine.NewContext(b.R)
	} else {
		b.Context = appengine.WithContext(b.Context, b.R)
	}

	return b.Next()
}
