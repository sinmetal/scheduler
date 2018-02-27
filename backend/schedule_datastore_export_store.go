package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mercari.io/datastore"
)

// ScheduleDatastoreExport is ScheduleDatastoreExport Model
type ScheduleDatastoreExport struct {
	Key           datastore.Key `json:"-" datastore:"-"`
	ProjectID     string        `json:"projectID"`
	Kinds         []string      `json:"kinds"`
	Bucket        string        `json:"bucket"`
	CreatedAt     time.Time     `json:"createdAt"`
	UpdatedAt     time.Time     `json:"updatedAt"`
	SchemaVersion int           `json:"-"`
}

var _ datastore.PropertyLoadSaver = &ScheduleDatastoreExport{}

// Load is Load
func (model *ScheduleDatastoreExport) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, model, ps)
}

// Save is Save
func (model *ScheduleDatastoreExport) Save(ctx context.Context) ([]datastore.Property, error) {
	model.SchemaVersion = 1
	if model.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	}
	model.UpdatedAt = time.Now()
	return datastore.SaveStruct(ctx, model)
}

// ScheduleDatastoreExportStore is Schedule Datastore Function
type ScheduleDatastoreExportStore struct{}

// Kind is Get Schedule Kind Name
func (store *ScheduleDatastoreExportStore) Kind() string {
	return "ScheduleDatastoreExport"
}

// NewKey is Create Schedule Model Key for New Entity
func (store *ScheduleDatastoreExportStore) NewKey(ctx context.Context, client datastore.Client) datastore.Key {
	return store.Key(ctx, client, uuid.New().String())
}

// Key is Create Schedule Model Key
func (store *ScheduleDatastoreExportStore) Key(ctx context.Context, client datastore.Client, id string) datastore.Key {
	return client.NameKey(store.Kind(), id, nil)
}

// Put is Schedule put to Datastore
func (store *ScheduleDatastoreExportStore) Put(ctx context.Context, key datastore.Key, schedule *ScheduleDatastoreExport) (*ScheduleDatastoreExport, error) {
	client, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}
	_, err = client.Put(ctx, key, schedule)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("datastore.Put: key = %v", key))
	}
	schedule.Key = key
	return schedule, nil
}

// ListAll is Get All Schedule
func (store *ScheduleDatastoreExportStore) ListAll(ctx context.Context) ([]*ScheduleDatastoreExport, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}

	var sl []*ScheduleDatastoreExport
	q := ds.NewQuery(store.Kind())
	kl, err := ds.GetAll(ctx, q, &sl)
	if err != nil {
		return nil, errors.Wrap(err, "datastore.GetAll")
	}

	for i, v := range kl {
		sl[i].Key = v
	}

	return sl, nil
}

// QueryByBucket is Bucketが一致するEntity Listを取得する
func (store *ScheduleDatastoreExportStore) QueryByBucket(ctx context.Context, bucket string) ([]*ScheduleDatastoreExport, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}

	var sl []*ScheduleDatastoreExport
	q := ds.NewQuery(store.Kind())
	q = q.Filter("Bucket =", bucket)
	kl, err := ds.GetAll(ctx, q, &sl)
	if err != nil {
		return nil, errors.Wrap(err, "datastore.GetAll")
	}

	for i, v := range kl {
		sl[i].Key = v
	}

	return sl, nil
}
