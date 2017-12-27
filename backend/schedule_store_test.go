package backend

import (
	"testing"

	"github.com/favclip/testerator"
)

func TestScheduleStore_Put(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleStore{}
	client, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	key := store.NewKey(ctx, client)
	ss, err := store.Put(ctx, key, &Schedule{})
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

func TestScheduleStore_ListAll(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatalf("%+v\n", err)
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleStore{}
	client, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	key := store.NewKey(ctx, client)
	_, err = store.Put(ctx, key, &Schedule{})
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
