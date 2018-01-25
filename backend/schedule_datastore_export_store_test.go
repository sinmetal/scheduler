package backend

import (
	"testing"

	"github.com/favclip/testerator"
)

func TestScheduleDatastoreExportStore_Put(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleDatastoreExportStore{}
	client, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	key := store.NewKey(ctx, client)
	ss, err := store.Put(ctx, key, &ScheduleDatastoreExport{})
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	if ss.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt.IsZero")
	}
	if ss.UpdatedAt.IsZero() {
		t.Fatalf("UpdatedAt.IsZero")
	}
}

func TestScheduleDatastoreExportStore_ListAll(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleDatastoreExportStore{}
	client, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	key := store.NewKey(ctx, client)
	_, err = store.Put(ctx, key, &ScheduleDatastoreExport{})
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	sl, err := store.ListAll(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	if e, g := 1, len(sl); e != g {
		t.Fatalf("expected ListAll results.length %d; got %d", e, g)
	}
}

func TestScheduleDatastoreExportStore_QueryByBucket(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleDatastoreExportStore{}
	client, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	const expectedBucket = "hoge"
	{
		key := store.NewKey(ctx, client)
		_, err = store.Put(ctx, key, &ScheduleDatastoreExport{
			Bucket: expectedBucket,
		})
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
	}
	{
		key := store.NewKey(ctx, client)
		_, err = store.Put(ctx, key, &ScheduleDatastoreExport{
			Bucket: "fuga",
		})
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
	}

	sl, err := store.QueryByBucket(ctx, expectedBucket)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	if e, g := 1, len(sl); e != g {
		t.Fatalf("expected QueryByBucket results.length %d; got %d", e, g)
	}
	if e, g := expectedBucket, sl[0].Bucket; e != g {
		t.Fatalf("expected Bucket %s; got %s", e, g)
	}
}
