package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.mercari.io/datastore"
)

// Schedule is Schedule Model
type Schedule struct {
	Key datastore.Key `json:"-" datastore:"-"`
	// TODO API Response用のKeyを入れる
	ProjectID       string    `json:"projectID"`
	DstProjectID    string    `json:"dstProjectID"`
	DstDatasetID    string    `json:"dstDatasetID"`
	DstTableID      string    `json:"dstTableID"`
	QueryPathBucket string    `json:"queryPathBucket"`
	QueryPathObject string    `json:"queryPathObject"`
	ScheduleV1      string    `json:"scheduleV1"`
	CreatedAt       time.Time `json:"createdAt"`
	UpdatedAt       time.Time `json:"updatedAt"`
	SchemaVersion   int       `json:"-"`
}

var _ datastore.PropertyLoadSaver = &Schedule{}

// Load is Load
func (model *Schedule) Load(ctx context.Context, ps []datastore.Property) error {
	return datastore.LoadStruct(ctx, model, ps)
}

// Save is Save
func (model *Schedule) Save(ctx context.Context) ([]datastore.Property, error) {
	model.SchemaVersion = 1
	if model.CreatedAt.IsZero() {
		model.CreatedAt = time.Now()
	}
	model.UpdatedAt = time.Now()
	return datastore.SaveStruct(ctx, model)
}

// ScheduleStore is Schedule Datastore Function
type ScheduleStore struct{}

// Kind is Get Schedule Kind Name
func (store *ScheduleStore) Kind() string {
	return "Schedule"
}

// NewKey is Create Schedule Model Key for New Entity
func (store *ScheduleStore) NewKey(ctx context.Context, client datastore.Client) datastore.Key {
	return store.Key(ctx, client, uuid.New().String())
}

// Key is Create Schedule Model Key
func (store *ScheduleStore) Key(ctx context.Context, client datastore.Client, id string) datastore.Key {
	return client.NameKey(store.Kind(), uuid.New().String(), nil)
}

// Put is Schedule put to Datastore
func (store *ScheduleStore) Put(ctx context.Context, key datastore.Key, schedule *Schedule) (*Schedule, error) {
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
func (store *ScheduleStore) ListAll(ctx context.Context) ([]*Schedule, error) {
	ds, err := fromContext(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "fromContext")
	}

	var sl []*Schedule
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
