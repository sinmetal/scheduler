package backend

import (
	"context"
	"time"

	"fmt"
	"github.com/pkg/errors"
	"go.mercari.io/datastore"
)

// ScheduleCloudSQLExportJob is Cloud SQL Exportを実行した時のJob
// Cloud SQL Exportを実行する度に1件生成される
type ScheduleCloudSQLExportJob struct {
	Key                 datastore.Key `json:"-" datastore:"-"`     // ExportURIをKey Nameとして利用する
	ProjectID           string        `json:"projectID"`           // ExportするCloudSQLが存在するProjectID
	Instance            string        `json:"instance"`            // ExportするCloudSQLのInstanceID
	Databases           []string      `json:"databases"`           // ExportするCloudSQLのDatabase
	SQLBucket           string        `json:"sqlBucket"`           // Export時に利用するSQLを置いているGCS Bucket. hoge
	SQLObject           string        `json:"sqlObject"`           // Export時に利用するSQLを置いているGCS Object. export.sql
	ExportURI           string        `json:"exportURI"`           // Export先のGCS Path gs://hoge/yyyyMMddhhmm/fuga.csv
	BigQueryProjectID   string        `json:"bigQueryProjectID"`   // Load先のBigQuery ProjectID
	BigQueryDataset     string        `json:"bigQueryDataset"`     // Load先のBigQuery Dataset
	BigQueryTable       string        `json:"bigQueryTable"`       // Load先のBigQuery Table
	BigQueryTableSchema string        `json:"bigQueryTableSchema"` // LoadするのBigQueryTableのSchema文字列 Ex. Name:STRING,Age:INTEGER
	CreatedAt           time.Time     `json:"createdAt"`
	UpdatedAt           time.Time     `json:"updatedAt"`
	SchemaVersion       int           `json:"-"`
}

var _ datastore.PropertyLoadSaver = &ScheduleCloudSQLExportJob{}

// Load is Load
func (model *ScheduleCloudSQLExportJob) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, model, ps)
}

// Save is Save
func (model *ScheduleCloudSQLExportJob) Save(ctx context.Context) ([]datastore.Property, error) {
	model.SchemaVersion = 1
	if model.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	}
	model.UpdatedAt = time.Now()
	return datastore.SaveStruct(ctx, model)
}

// ScheduleCloudSQLExportJobStore is Schedule Cloud SQL Export Job Function
type ScheduleCloudSQLExportJobStore struct{}

// Kind is Get ScheduleCloudSQLExportJob Kind Name
func (store *ScheduleCloudSQLExportJobStore) Kind() string {
	return "ScheduleCloudSQLExportJob"
}

// Key is Create ScheduleCloudSQLExportJob Key
// idにはExportURIを指定する
func (store *ScheduleCloudSQLExportJobStore) Key(ctx context.Context, client datastore.Client, id string) datastore.Key {
	return client.NameKey(store.Kind(), id, nil)
}

// Put is ScheduleCloudSQLExportJob をDatastoreにPutする
func (store *ScheduleCloudSQLExportJobStore) Put(ctx context.Context, job *ScheduleCloudSQLExportJob) (*ScheduleCloudSQLExportJob, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}
	key := store.Key(ctx, ds, job.ExportURI)
	_, err = ds.Put(ctx, key, job)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("datastore.Put: key = %v", key))
	}
	job.Key = key
	return job, nil
}

// Get is ScheduleCloudSQLExportJob をDatastoreからGetする
func (store *ScheduleCloudSQLExportJobStore) Get(ctx context.Context, key datastore.Key) (*ScheduleCloudSQLExportJob, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}

	var e ScheduleCloudSQLExportJob
	if err := ds.Get(ctx, key, &e); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("datastore.Get key = %v", key))
	}
	e.Key = key
	return &e, nil
}
