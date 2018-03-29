package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mercari.io/datastore"
)

// StorageBQLoadConfig is Cloud Storage上に置かれたFileをBigQueryにLoadするための設定
// File名と同じTable名 かつ スキーマはAutoDetectで設定するのを前提にしているので、設定項目は最小限
type StorageBQLoadConfig struct {
	Key                  datastore.Key `json:"-" datastore:"-"`
	FrmStorageBucket     string        `json:"frmStorageBucket"`     // LoadするFileがあるCloud Storage Bucket
	DstBigQueryProjectID string        `json:"dstBigQueryProjectID"` // Load先のBigQueryのProjectID
	DstBigQueryDataset   string        `json:"dstBigQueryDataset"`   // Load先のBigQueryのDataset
	CreatedAt            time.Time     `json:"createdAt"`
	UpdatedAt            time.Time     `json:"updatedAt"`
	SchemaVersion        int           `json:"-"`
}

var _ datastore.PropertyLoadSaver = &StorageBQLoadConfig{}

// Load is Load
func (model *StorageBQLoadConfig) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, model, ps)
}

// Save is Save
func (model *StorageBQLoadConfig) Save(ctx context.Context) ([]datastore.Property, error) {
	model.SchemaVersion = 1
	if model.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	}
	model.UpdatedAt = time.Now()
	return datastore.SaveStruct(ctx, model)
}

// StorageBQLoadConfigStore is StorageBQLoadConfig Functions
type StorageBQLoadConfigStore struct{}

// Kind is Get StorageBQLoadConfigStore Kind Name
func (store *StorageBQLoadConfigStore) Kind() string {
	return "StorageBQLoadConfig"
}

// NewKey is Create StorageBQLoadConfig Key
// KeyNameとしてUUIDを利用
func (store *StorageBQLoadConfigStore) NewKey(ctx context.Context, client datastore.Client) datastore.Key {
	return client.NameKey(store.Kind(), uuid.New().String(), nil)
}

// Key is Create StorageBQLoadConfig Key
func (store *StorageBQLoadConfigStore) Key(ctx context.Context, client datastore.Client, id string) datastore.Key {
	return client.NameKey(store.Kind(), id, nil)
}

// Put is StorageBQLoadConfig をDatastoreにPutする
func (store *StorageBQLoadConfigStore) Put(ctx context.Context, key datastore.Key, config *StorageBQLoadConfig) (*StorageBQLoadConfig, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}
	_, err = ds.Put(ctx, key, config)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("datastore.Put: key = %v", key))
	}
	config.Key = key

	return config, nil
}
