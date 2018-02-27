package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
)

// ScheduleCloudSQLExportAPI is CloudSQLのExport ConfigのScheduleのためのAPI
type ScheduleCloudSQLExportAPI struct{}

func setupScheduleCloudSQLExportAPI(swPlugin *swagger.Plugin) {
	api := &ScheduleCloudSQLExportAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "Cloud SQL Export Config", Description: "Cloud SQL Export Config API list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/scheduleCloudSQLExport", hInfo)
	hInfo.Description, hInfo.Tags = "post to Cloud SQL Export Config", []string{tag.Name}
}

// ScheduleCloudSQLExportAPIPostRequest is Post Param
type ScheduleCloudSQLExportAPIPostRequest struct {
	ProjectID string   `json:"projectID"` // ExportするCloudSQLが存在するProjectID
	Databases []string `json:"databases"` // ExportするCloudSQLのDatabase
	SQLURI    string   `json:"sqlURI"`    // Export時に利用するSQLを置いているGCS Path. gs://hoge/fuga.csv
	ExportURI string   `json:"exportURI"` // Export先のGCS Path. %sを入れるとyyyyMMddhhmmに置き換える gs://hoge/%s/fuga.csv
}

// ScheduleCloudSQLExportAPIPostResponse is Post Response
type ScheduleCloudSQLExportAPIPostResponse struct {
	*ScheduleCloudSQLExport
}

// Post is ScheduleCloudSQLExportAPI Post Handler
// ScheduleCloudSQLExportを新規にDatastoreにPutする
func (api *ScheduleCloudSQLExportAPI) Post(ctx context.Context, form *ScheduleCloudSQLExportAPIPostRequest) (*ScheduleCloudSQLExportAPIPostResponse, error) {
	store := ScheduleCloudSQLExportStore{}

	ds, err := fromContext(ctx)
	if err != nil {
		return nil, err
	}

	key := store.NewKey(ctx, ds)
	s := ScheduleCloudSQLExport{
		ProjectID: form.ProjectID,
		Databases: form.Databases,
		SQLURI:    form.SQLURI,
		ExportURI: form.ExportURI,
	}
	ss, err := store.Put(ctx, key, &s)
	if err != nil {
		return nil, err
	}
	return &ScheduleCloudSQLExportAPIPostResponse{ss}, nil
}
