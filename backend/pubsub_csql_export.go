package backend

import (
	"context"
	"fmt"
	"net/http"

	"github.com/sinmetal/pubsub"
	"go.mercari.io/datastore"
	"google.golang.org/appengine/log"
)

// ReceiveCloudSQLExportPubSubHandler is Cloud SQL ExportのファイルCloud Storageに入った時のCloud Pus/SubのPushを受け取って解釈するHandler
func ReceiveCloudSQLExportPubSubHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	for k, v := range r.Header {
		log.Infof(ctx, "%s:%s", k, v)
	}

	msg, err := pubsub.ReadBody(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Errorf(ctx, "%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Infof(ctx, "%+v", msg)

	if pubsub.ObjectFinalize != msg.Message.Attributes.EventType {
		log.Infof(ctx, "gs://%s/%s is not exists", msg.Message.Attributes.BucketID, msg.Message.Attributes.ObjectID)
		w.WriteHeader(http.StatusOK)
		return
	}

	ds, err := fromContext(ctx)
	if err != nil {
		log.Errorf(ctx, "failed fromContext")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	gp := fmt.Sprintf("gs://%s/%s", msg.Message.Attributes.BucketID, msg.Message.Attributes.ObjectID)
	store := ScheduleCloudSQLExportJobStore{}
	key := store.Key(ctx, ds, gp)
	job, err := store.Get(ctx, key)
	if err == datastore.ErrNoSuchEntity {
		log.Warningf(ctx, "notfound job entity. obj = gs://%s/%s", msg.Message.Attributes.BucketID, msg.Message.Attributes.ObjectID)
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

		// PubSubがリトライし続けてしまうので、200 OKを返す
		// TODO 何か通知があった方がいいかも？
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
}
