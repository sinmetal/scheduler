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
// +qgb
type ScheduleCloudSQLExport struct {
	Key           datastore.Key `json:"-" datastore:"-"`
	ProjectID     string        `json:"projectID"` // ExportするCloudSQLが存在するProjectID
	Instance      string        `json:"instance"`  // ExportするCloudSQLのInstanceID
	Databases     []string      `json:"databases"` // ExportするCloudSQLのDatabase
	SQLBucket     string        `json:"sqlBucket"` // Export時に利用するSQLを置いているGCS Bucket. hoge
	SQLObject     string        `json:"sqlObject"` // Export時に利用するSQLを置いているGCS Object. export.sql
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
	return "ScheduleCloudSQLExport"
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

// ListAll is ScheduleCloudSQLExportStore を全件取得する
func (store *ScheduleCloudSQLExportStore) ListAll(ctx context.Context) ([]*ScheduleCloudSQLExport, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}

	var sl []*ScheduleCloudSQLExport
	b := NewScheduleCloudSQLExportQueryBuilder(ds)
	kl, err := ds.GetAll(ctx, b.Query(), &sl)
	if err != nil {
		return nil, errors.Wrap(err, "datastore.GetAll")
	}

	for i, v := range kl {
		sl[i].Key = v
	}

	return sl, nil
}
