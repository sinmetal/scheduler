package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.mercari.io/datastore"
	"google.golang.org/appengine/log"
)

// CloudSQLExportMutex is Cloud SQL ExportがInstanceごとに1つしか同時実行できないので、実行を管理するためのKind
//
type CloudSQLExportMutex struct {
	Key                          datastore.Key `datastore:"-"` // Cloud SQL Instance ID
	ScheduleCloudSQLExportJobKey datastore.Key // Mutexを掴んでいるJobのKey
	CreatedAt                    time.Time     `json:"createdAt"`
	UpdatedAt                    time.Time     `json:"updatedAt"`
	SchemaVersion                int           `json:"-"`
}

// ErrCloudSQLExportMutexCollision is すでに誰かが掴んでいる時に返すエラー
var ErrCloudSQLExportMutexCollision = errors.New("collision cloud sql export mutex")

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

// Key is Create CloudSQLExportMutex Key
func (store *CloudSQLExportMutexStore) Key(ctx context.Context, client datastore.Client, projectID string, instance string) datastore.Key {
	return client.NameKey(store.Kind(), fmt.Sprintf("%s-_-%s", projectID, instance), nil)
}

// Lock is Make the state that grabbed the specified mutex. If already grabbed, ErrCloudSQLExportMutexCollision is returned.
// 指定したMutexを掴んだ状態にする。すでに掴まれていた場合は、ErrCloudSQLExportMutexCollision が返ってくる。
func (store *CloudSQLExportMutexStore) Lock(ctx context.Context, projectID string, instance string, jobKey datastore.Key) error {
	log.Infof(ctx, "Try Lock CloudSQLExportMutex!! %s/%s", projectID, instance)

	ds, err := fromContext(ctx)
	if err != nil {
		errors.Wrap(err, "failed fromContext")
	}

	key := store.Key(ctx, ds, projectID, instance)
	_, err = ds.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		var e CloudSQLExportMutex
		if err := tx.Get(key, &e); err != nil {
			if err == datastore.ErrNoSuchEntity {
				// new mutex !
				e = CloudSQLExportMutex{}
			} else {
				return errors.Wrap(err, fmt.Sprintf("failed datastore.Get. key = %v", key))
			}
		}
		if e.ScheduleCloudSQLExportJobKey != nil && e.ScheduleCloudSQLExportJobKey.Equal(jobKey) == false {
			// 誰かが使ってる！
			return ErrCloudSQLExportMutexCollision
		}

		e.ScheduleCloudSQLExportJobKey = jobKey
		_, err = tx.Put(key, &e)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed datastore.Put. key = %v", key))
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed datastore.transaction. key = %v", key))
	}
	log.Infof(ctx, "Complate Lock CloudSQLExportMutex!! %s/%s", projectID, instance)
	return nil
}

// Unlock is Release the grabbing Mutex.
// 掴んでいるMutexを解放する。
func (store *CloudSQLExportMutexStore) Unlock(ctx context.Context, projectID string, instance string, jobKey datastore.Key) error {
	ds, err := fromContext(ctx)
	if err != nil {
		errors.Wrap(err, "failed fromContext")
	}

	key := store.Key(ctx, ds, projectID, instance)
	_, err = ds.RunInTransaction(ctx, func(tx datastore.Transaction) error {
		var e CloudSQLExportMutex
		if err := tx.Get(key, &e); err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed datastore.Get. key = %v", key))
		}
		if e.ScheduleCloudSQLExportJobKey != nil && e.ScheduleCloudSQLExportJobKey.Equal(jobKey) == false {
			// 誰かが使ってる！
			return ErrCloudSQLExportMutexCollision
		}

		e.ScheduleCloudSQLExportJobKey = nil
		_, err = tx.Put(key, &e)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed datastore.Put. key = %v", key))
		}
		return nil
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed datastore.transaction. key = %v", key))
	}
	return nil
}
