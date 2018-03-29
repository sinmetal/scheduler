package backend

import (
	"context"
	"fmt"
	"time"

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

// Key is Create StorageBQLoadConfig Key
// Keyの値にはCloud StorageのBucketを利用する
// OCNを受け取った時に、どこにLoadするかを見るためにEntityを取得するため
func (store *StorageBQLoadConfigStore) Key(ctx context.Context, client datastore.Client, bucket string) datastore.Key {
	return client.NameKey(store.Kind(), bucket, nil)
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

// Get is StorageBQLoadConfig をDatastoreからGetする
func (store *StorageBQLoadConfigStore) Get(ctx context.Context, key datastore.Key) (*StorageBQLoadConfig, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}

	var e StorageBQLoadConfig
	if err := ds.Get(ctx, key, &e); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("datastore.Get. key=%+v", key))
	}
	e.Key = key
	return &e, nil
}
