package backend

import (
	"github.com/favclip/testerator"
	"testing"
)

// TestScheduleCloudSQLExportJobStore_Put is DatastoreにEntityをPutできることを確認
func TestScheduleCloudSQLExportJobStore_Put(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleCloudSQLExportJobStore{}
	job := ScheduleCloudSQLExportJob{
		ProjectID: "hogeproject",
		Instance:  "sql1",
		Databases: []string{"db1"},
		SQLBucket: "hogebucket",
		SQLObject: "sample.sql",
		ExportURI: "gs://hoge/20180305164515/sample.csv",
	}
	stored, err := store.Put(ctx, &job)
	if err != nil {
		t.Fatal(err)
	}

	if stored.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt.IsZero")
	}
	if stored.UpdatedAt.IsZero() {
		t.Fatalf("UpdatedAt.IsZero")
	}
}

// TestScheduleCloudSQLExportJobStore_Get is DatastoreからEntityをGetできることを確認
func TestScheduleCloudSQLExportJobStore_Get(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	store := ScheduleCloudSQLExportJobStore{}
	job := ScheduleCloudSQLExportJob{
		ProjectID: "hogeproject",
		Instance:  "sql1",
		Databases: []string{"db1"},
		SQLBucket: "hogebucket",
		SQLObject: "sample.sql",
		ExportURI: "gs://hoge/20180305164515/sample.csv",
	}
	_, err = store.Put(ctx, &job)
	if err != nil {
		t.Fatal(err)
	}

	key := store.Key(ctx, ds, job.ExportURI)
	_, err = store.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
}
