package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ds2bq"
	"google.golang.org/appengine/log"
)

// ReceiveOCNHandler is Datastore ExportのファイルがCloud Storageに入った時にBigQueryにUploadするためのOCNのHandler
func ReceiveOCNHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	const tqURL = "/tq/gcs/object-to-bq"

	obj, err := ds2bq.DecodeGCSObject(r.Body)
	if err != nil {
		log.Errorf(ctx, "ds2bq: failed to decode request: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	store := ScheduleDatastoreExportStore{}
	sl, err := store.QueryByBucket(ctx, obj.Bucket)
	if err != nil {
		log.Errorf(ctx, "Datastore: QueryByBucket: Buckdet = %s; %v", obj.Bucket, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if len(sl) != 1 {
		log.Errorf(ctx, "Query Results ScheduleDatastoreExport.length != 1; got %d; bucket %s", len(sl), obj.Bucket)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if !obj.IsImportTarget(ctx, r, obj.Bucket, sl[0].Kinds) {
		w.WriteHeader(http.StatusOK)
		return
	}

	err = ds2bq.ReceiveOCN(ctx, obj, "ds2bq", tqURL)
	if err != nil {
		log.Errorf(ctx, "ds2bq: failed to receive OCN: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
