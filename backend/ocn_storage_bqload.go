package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/favclip/ds2bq"
	"go.mercari.io/datastore"
	"google.golang.org/appengine/log"
)

// ReceiveStorageBQLoadOCNHandler is Cloud Storage load to BigQueryのOCNを受け取るためのHandler
func ReceiveStorageBQLoadOCNHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	h := ds2bq.NewGCSHeader(r)
	log.Infof(ctx, "header: %+v", h)

	obj, err := ds2bq.DecodeGCSObject(r.Body)
	if err != nil {
		log.Errorf(ctx, "ds2bq: failed to decode request: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	j, err := json.Marshal(obj)
	if err != nil {
		log.Errorf(ctx, "OCN Body to json.Marshal: %s", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Infof(ctx, "obj:%s\n", j)

	if h.ResourceState != "exists" {
		log.Infof(ctx, "gs://%s/%s is not exists", obj.Bucket, obj.Name)
		w.WriteHeader(http.StatusOK)
		return
	}

	ds, err := fromContext(ctx)
	if err != nil {
		log.Errorf(ctx, "failed fromContext")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	store := StorageBQLoadConfigStore{}
	key := store.Key(ctx, ds, obj.Bucket)
	config, err := store.Get(ctx, key)
	if err == datastore.ErrNoSuchEntity {
		log.Warningf(ctx, "notfound job entity. obj = %s", obj.SelfLink)
		// TODO 何か通知があった方がいいかも？
		w.WriteHeader(http.StatusOK)
		return
	} else if err != nil {
		log.Errorf(ctx, "failed StorageBQLoadConfigStore.Get. Key=%v,err=%+v", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	t := strings.Replace(obj.Name, ".csv", "", -1)
	bq := BigQueryService{}
	if err := bq.LoadWithAutodetect(ctx, &BigQueryLoadWithAutodetectParam{
		CloudStorageURI:   fmt.Sprintf("gs://%s/%s", obj.Bucket, obj.Name),
		BigQueryProjectID: config.DstBigQueryProjectID,
		BigQueryDataset:   config.DstBigQueryDataset,
		BigQueryTable:     t,
		SourceFormat:      "CSV",
	}); err != nil {
		log.Errorf(ctx, "failed bigquery.LoadWithAutodetect. ConfigKey=%v, err=%+v", key, err)

		// OCNがリトライし続けてしまうので、200 OKを返す
		// TODO 何か通知があった方がいいかも？
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
}
