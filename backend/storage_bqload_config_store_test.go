package backend

import (
	"testing"

	"github.com/favclip/testerator"
)

func TestStorageBQLoadConfigStore_Put(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする
	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	store := StorageBQLoadConfigStore{}

	entity := &StorageBQLoadConfig{
		FrmStorageBucket:     "hoge",
		DstBigQueryProjectID: "hogeprojectid",
		DstBigQueryDataset:   "hogedataset",
	}

	key := store.NewKey(ctx, ds)
	stored, err := store.Put(ctx, key, entity)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Key == nil {
		t.Fatal("expected Key is not nil")
	}
	if e, g := entity.FrmStorageBucket, stored.FrmStorageBucket; e != g {
		t.Fatalf("expected FrmStorageBucket is %s; got %s", e, g)
	}
	if e, g := entity.DstBigQueryProjectID, stored.DstBigQueryProjectID; e != g {
		t.Fatalf("expected DstBigQueryProjectID is %s; got %s", e, g)
	}
	if e, g := entity.DstBigQueryDataset, stored.DstBigQueryDataset; e != g {
		t.Fatalf("expected DstBigQueryDataset is %s; got %s", e, g)
	}
	if stored.CreatedAt.IsZero() {
		t.Fatal("expected CreatedAt is Not Zero")
	}
	if stored.UpdatedAt.IsZero() {
		t.Fatal("expected UpdatedAt is Not Zero")
	}
	if e, g := 1, stored.SchemaVersion; e != g {
		t.Fatalf("expected SchemaVersion is %d; got %d", e, g)
	}
}
