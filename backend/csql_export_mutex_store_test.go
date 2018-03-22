package backend

import (
	"testing"

	"github.com/favclip/testerator"
)

func TestCloudSQLExportMutexStore_Lock(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	jobstore := ScheduleCloudSQLExportJobStore{}
	mustore := CloudSQLExportMutexStore{}

	const projectID = "hoge"
	const instance = "fuga"

	jobKey := jobstore.Key(ctx, ds, "gs://hoge/fuga.csv")
	err = mustore.Lock(ctx, projectID, instance, jobKey)
	if err != nil {
		t.Fatal(err)
	}

	var mu CloudSQLExportMutex
	muKey := mustore.Key(ctx, ds, projectID, instance)
	err = ds.Get(ctx, muKey, &mu)
	if err != nil {
		t.Fatal(err)
	}

	if jobKey.Equal(mu.ScheduleCloudSQLExportJobKey) == false {
		t.Fatalf("expected Set ScheduleCloudSQLExportJobKey")
	}
}

func TestCloudSQLExportMutexStore_Unlock(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	jobstore := ScheduleCloudSQLExportJobStore{}
	mustore := CloudSQLExportMutexStore{}

	const projectID = "hoge"
	const instance = "fuga"

	jobKey := jobstore.Key(ctx, ds, "gs://hoge/fuga.csv")
	err = mustore.Lock(ctx, projectID, instance, jobKey)
	if err != nil {
		t.Fatal(err)
	}

	if err := mustore.Unlock(ctx, projectID, instance, jobKey); err != nil {
		t.Fatal(err)
	}

	var mu CloudSQLExportMutex
	muKey := mustore.Key(ctx, ds, projectID, instance)
	err = ds.Get(ctx, muKey, &mu)
	if err != nil {
		t.Fatal(err)
	}

	if mu.ScheduleCloudSQLExportJobKey != nil {
		t.Fatalf("expected ScheduleCloudSQLExportJobKey is nil")
	}
}
