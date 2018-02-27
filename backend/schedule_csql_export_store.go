package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mercari.io/datastore"
)

// ScheduleCloudSQLExport is Cloud SQLをExportするScheduleの設定
type ScheduleCloudSQLExport struct {
	Key           datastore.Key `json:"-" datastore:"-"`
	ProjectID     string        `json:"projectID"` // ExportするCloudSQLが存在するProjectID
	Databases     []string      `json:"databases"` // ExportするCloudSQLのDatabase
	SQLURI        string        `json:"sqlURI"`    // Export時に利用するSQLを置いているGCS Path. gs://hoge/fuga.csv
	ExportURI     string        `json:"exportURI"` // Export先のGCS Path. %sを入れるとyyyyMMddhhmmに置き換える gs://hoge/%s/fuga.csv
	CreatedAt     time.Time     `json:"createdAt"`
	UpdatedAt     time.Time     `json:"updatedAt"`
	SchemaVersion int           `json:"-"`
}

var _ datastore.PropertyLoadSaver = &ScheduleCloudSQLExport{}

// Load is Load
func (model *ScheduleCloudSQLExport) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, model, ps)
}

// Save is Save
func (model *ScheduleCloudSQLExport) Save(ctx context.Context) ([]datastore.Property, error) {
	model.SchemaVersion = 1
	if model.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	}
	model.UpdatedAt = time.Now()
	return datastore.SaveStruct(ctx, model)
}

// ScheduleCloudSQLExportStore is Schedule Cloud SQL Export Function
type ScheduleCloudSQLExportStore struct{}

// Kind is Get ScheduleCloudSQLExport Kind Name
func (store *ScheduleCloudSQLExportStore) Kind() string {
	return "ScheduleCloudSQLExportStore"
}

// NewKey is Create ScheduleCloudSQLExport Model Key
func (store *ScheduleCloudSQLExportStore) NewKey(ctx context.Context, client datastore.Client) datastore.Key {
	return store.Key(ctx, client, uuid.New().String())
}

// Key is Create ScheduleCloudSQLExport Key
func (store *ScheduleCloudSQLExportStore) Key(ctx context.Context, client datastore.Client, id string) datastore.Key {
	return client.NameKey(store.Kind(), id, nil)
}

// Put is ScheduleCloudSQLExportをDatastoreにPutする
func (store *ScheduleCloudSQLExportStore) Put(ctx context.Context, key datastore.Key, schedule *ScheduleCloudSQLExport) (*ScheduleCloudSQLExport, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}
	_, err = ds.Put(ctx, key, schedule)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("datastore.Put: key = %v", key))
	}
	schedule.Key = key
	return schedule, nil
}
