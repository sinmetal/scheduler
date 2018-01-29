package backend

import (
	"net/http"
	"reflect"
	"strings"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"google.golang.org/appengine"
)

func init() {
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

	setupBigQuery(swPlugin)
	setUpBatch(swPlugin)
	setupScheduleAPI(swPlugin)
	setupScheduleDatastoreExportAPI(swPlugin)
	setUpTQBuildeQueryAPI(swPlugin)
	setupTQDatastoreExportAPI(swPlugin)
	setupCronQueryAPI(swPlugin)
	setupCronDatastoreExportAPI(swPlugin)

	ucon.HandleFunc(http.MethodPost, "/ocn/datastore-export", ReceiveOCNHandler)
	ucon.HandleFunc(http.MethodPost, "/tq/gcs/object-to-bq", ImportBigQueryHandleFunc("datastore_imports"))

	ucon.DefaultMux.Prepare()
	http.Handle("/", ucon.DefaultMux)
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
