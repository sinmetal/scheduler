package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
)

type ScheduleAPI struct{}

func setupScheduleAPI(swPlugin *swagger.Plugin) {
	api := &ScheduleAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Schedule", Description: "Schedule list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/schedule", hInfo)
	hInfo.Description, hInfo.Tags = "post to schedule", []string{tag.Name}
}

func (api *ScheduleAPI) Post(ctx context.Context) {

}
