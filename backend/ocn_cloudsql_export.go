package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/favclip/ds2bq"
	"go.mercari.io/datastore"
	"google.golang.org/appengine/log"
)

// ReceiveCloudSQLExportOCNHandler is Cloud SQL ExportのファイルがCloud Storageに入った時にOCNを受け取り、BigQueryにUploadするためのHandler
func ReceiveCloudSQLExportOCNHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h := ds2bq.NewGCSHeader(r)
	log.Infof(ctx, "header: %+v", h)

	obj, err := ds2bq.DecodeGCSObject(r.Body)
	if err != nil {
		log.Errorf(ctx, "ds2bq: failed to decode request: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if h.ResourceState != "exists" {
		log.Infof(ctx, "gs://%s/%s is not exists", obj.Bucket, obj.Name)
		w.WriteHeader(http.StatusOK)
		return
	}

	j, err := json.Marshal(obj)
	if err != nil {
		log.Errorf(ctx, "OCN Body to json.Marshal: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Infof(ctx, "obj:%s\n", j)

	ds, err := fromContext(ctx)
	if err != nil {
		log.Errorf(ctx, "failed fromContext")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	gp := fmt.Sprintf("gs://%s/%s", obj.Bucket, obj.Name)
	store := ScheduleCloudSQLExportJobStore{}
	key := store.Key(ctx, ds, gp)
	job, err := store.Get(ctx, key)
	if err == datastore.ErrNoSuchEntity {
		log.Warningf(ctx, "notfound job entity. obj = %s", obj.SelfLink)
		// TODO 何か通知があった方がいいかも？
		w.WriteHeader(http.StatusOK)
		return
	} else if err != nil {
		log.Errorf(ctx, "failed ScheduleCloudSQLExportJobStore.Get. Key = %v", key)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	bqs := BigQueryService{}
	if err := bqs.LoadFromCloudSQLExport(ctx, job); err != nil {
		log.Errorf(ctx, "failed bigquery.LoadFromCloudSQLExport. JobEntityKey = %v", key)

		// OCNがリトライし続けてしまうので、200 OKを返す
		// TODO 何か通知があった方がいいかも？
		w.WriteHeader(http.StatusOK)
		return
	}

	w.WriteHeader(http.StatusOK)
}
