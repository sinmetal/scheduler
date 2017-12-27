package backend

import (
	"testing"

	"github.com/favclip/testerator"
)

func TestScheduleStore_Put(t *testing.T) {
	_, ctx, err := testerator.SpinUp() // gae/pythonのインスタンスが無ければ起動、あれば使いまわす
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown() // プロセスをシャットダウンせずに、Datastoreなどの内容をクリアする

	store := ScheduleStore{}
	client, err := fromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	key := store.NewKey(ctx, client)
	ss, err := store.Put(ctx, key, &Schedule{})
	if err != nil {
		t.Fatal(err)
	}

	if ss.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt.IsZero")
	}
	if ss.UpdatedAt.IsZero() {
		t.Fatalf("UpdatedAt.IsZero")
	}
}
