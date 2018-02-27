package backend

import (
	"testing"

	"github.com/favclip/testerator"
)

func TestScheduleCloudSQLExportStore_Put(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleCloudSQLExportStore{}
	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	key := store.NewKey(ctx, ds)
	ss, err := store.Put(ctx, key, &ScheduleCloudSQLExport{})
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
