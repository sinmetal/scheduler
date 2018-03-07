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
	ProjectID           string   `json:"projectID"`           // ExportするCloudSQLが存在するProjectID
	Instance            string   `json:"instance"`            // ExportするCloudSQLのInstanceID
	Databases           []string `json:"databases"`           // ExportするCloudSQLのDatabase
	SQLBucket           string   `json:"sqlBucket"`           // Export時に利用するSQLを置いているGCS Bucket. hoge
	SQLObject           string   `json:"sqlObject"`           // Export時に利用するSQLを置いているGCS Object. export.sql
	ExportURI           string   `json:"exportURI"`           // Export先のGCS Path. %sを入れるとyyyyMMddhhmmに置き換える gs://hoge/%s/fuga.csv
	BigQueryProjectID   string   `json:"bigQueryProjectID"`   // Load先のBigQuery ProjectID
	BigQueryDataset     string   `json:"bigQueryDataset"`     // Load先のBigQuery Dataset
	BigQueryTable       string   `json:"bigQueryTable"`       // Load先のBigQuery Table
	BigQueryTableSchema string   `json:"bigQueryTableSchema"` // LoadするのBigQueryTableのSchema文字列 Ex. Name:STRING,Age:INTEGER
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
		ProjectID:           form.ProjectID,
		Instance:            form.Instance,
		Databases:           form.Databases,
		SQLBucket:           form.SQLBucket,
		SQLObject:           form.SQLObject,
		ExportURI:           form.ExportURI,
		BigQueryProjectID:   form.ProjectID,
		BigQueryDataset:     form.BigQueryDataset,
		BigQueryTable:       form.BigQueryTable,
		BigQueryTableSchema: form.BigQueryTableSchema,
	}
	ss, err := store.Put(ctx, key, &s)
	if err != nil {
		return nil, err
	}
	return &ScheduleCloudSQLExportAPIPostResponse{ss}, nil
}
