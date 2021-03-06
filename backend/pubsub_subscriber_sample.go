package backend

import (
	"context"
	"net/http"

	"github.com/sinmetal/pubsub"
	"google.golang.org/appengine/log"
)

// ReceivePubSubSampleHandler is Cloud Pus/SubのPushを受け取って解釈するHandler
func ReceivePubSubSampleHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
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
	w.WriteHeader(http.StatusOK)
}
