package backend

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

// CronQueryAPI is Cron run to Query API
type CronQueryAPI struct{}

func SetupCronQueryAPI(swPlugin *swagger.Plugin) {
	api := &CronQueryAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Cron", Description: "Cron list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Get)
	ucon.Handle(http.MethodGet, "/cron/query", hInfo)
	hInfo.Description, hInfo.Tags = "run to cron query", []string{tag.Name}
}

// Get is CronQueryAPI Get Handler
func (api *CronQueryAPI) Get(ctx context.Context) error {
	store := ScheduleStore{}
	l, err := store.ListAll(ctx)
	if err != nil {
		log.Errorf(ctx, "%+v", err)
		return err
	}

	const queueName = "build-query"
	// TODO 日時指定するオプションを追加
	yesterday := time.Now().AddDate(0, 0, -1)
	var tasks []*taskqueue.Task
	for i, v := range l {
		// TODO 実行すべきかのハンドリングを追加

		body := TQBuildQueryAPIPostRequest{
			ProjectID:         v.ProjectID,
			DstProjectID:      v.DstProjectID,
			DstDatasetID:      v.DstDatasetID,
			DstTableID:        v.DstTableID,
			QueryPathBucket:   v.QueryPathBucket,
			QueryPathObject:   v.QueryPathObject,
			CreateDisposition: "",
			TargetDate:        yesterday,
		}
		b, err := json.Marshal(body)
		if err != nil {
			log.Errorf(ctx, "json.Marshal :%+v", err)
			return err
		}

		tasks = append(tasks,
			&taskqueue.Task{
				Path: "/tq/buildQuery",
				Header: http.Header{
					"Content-Type": []string{"application/json"},
				},
				Method:  http.MethodPost,
				Payload: b,
			})
		if len(tasks) > 93 {
			_, err = taskqueue.AddMulti(ctx, tasks, queueName)
			if err != nil {
				log.Errorf(ctx, "taskqueue.AddMulti :%+v", err)
				return err
			}
			tasks = []*taskqueue.Task{}
			log.Infof(ctx, "taskqueue.AddMulti: count=%d", i)
		}
	}
	if len(tasks) > 0 {
		_, err = taskqueue.AddMulti(ctx, tasks, queueName)
		if err != nil {
			log.Errorf(ctx, "taskqueue.AddMulti :%v", err)
			return err
		}
	}

	return nil
}
