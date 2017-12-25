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
	Key             datastore.Key `json:"-" datastore:"-"`
	ProjectID       string        `json:"projectID"`
	DstProjectID    string        `json:"dstProjectID"`
	DstDatasetID    string        `json:"dstDatasetID"`
	DstTableID      string        `json:"dstTableID"`
	Query           string        `json:"query"`
	QueryPathBucket string        `json:"queryPathBucket"`
	QueryPathObject string        `json:"queryPathObject"`
	CreatedAt       time.Time     `json:"createdAt"`
	UpdatedAt       time.Time     `json:"updatedAt"`
	SchemaVersion   int           `json:"-"`
}

var _ datastore.PropertyLoadSaver = &Schedule{}

func (model *Schedule) Load(ctx context.Context, ps []datastore.Property) error {
	if err := datastore.LoadStruct(ctx, model, ps); err != nil {
		return err
	}

	return nil
}

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

func (store *ScheduleStore) NewKey(ctx context.Context, client datastore.Client) datastore.Key {
	return store.Key(ctx, client, uuid.New().String())
}

func (store *ScheduleStore) Key(ctx context.Context, client datastore.Client, id string) datastore.Key {
	return client.NameKey("Schedule", uuid.New().String(), nil)
}

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
