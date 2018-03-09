package backend

import (
	"time"

	"go.mercari.io/datastore"
	"context"
)

// CloudSQLExportMutex is Cloud SQL ExportがInstanceごとに1つしか同時実行できないので、実行を管理するためのKind
//
type CloudSQLExportMutex struct {
	Key datastore.Key `datastore:"-"` // Cloud SQL Instance ID
	ScheduleCloudSQLExportJobKey datastore.Key // Mutexを掴んでいるJobのKey
	CreatedAt           time.Time     `json:"createdAt"`
	UpdatedAt           time.Time     `json:"updatedAt"`
	SchemaVersion       int           `json:"-"`
}

var _ datastore.PropertyLoadSaver = &CloudSQLExportMutex{}

// Load is Load
func (model *CloudSQLExportMutex) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, model, ps)
}

// Save is Save
func (model *CloudSQLExportMutex) Save(ctx context.Context) ([]datastore.Property, error) {
	model.SchemaVersion = 1
	if model.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	}
	model.UpdatedAt = time.Now()
	return datastore.SaveStruct(ctx, model)
}

// CloudSQLExportMutexStore is CloudSQLExportMutex Function
type CloudSQLExportMutexStore struct{}

// Kind is Get CloudSQLExportMutex Kind Name
func (store *CloudSQLExportMutexStore) Kind() string {
	return "CloudSQLExportMutex"
}

func (store *CloudSQLExportMutexStore) Lock(ctx context.Context) {

}

func (store *CloudSQLExportMutexStore) Unlock(ctx context.Context) {

}