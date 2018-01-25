package backend

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/favclip/testerator"
)

func TestCronDatastoreExportAPI_Get(t *testing.T) {
	inst, ctx, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	mock := MockTQService{}
	SetTaskQueueService(&mock)

	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}

	// Scheduleとして1件登録しておく
	store := ScheduleDatastoreExportStore{}
	key := store.NewKey(ctx, ds)
	schedule := &ScheduleDatastoreExport{
		ProjectID: "hogeproject",
		Kinds:     []string{"hoge", "fuga"},
		Bucket:    "hogebucket",
	}
	_, err = store.Put(ctx, key, schedule)
	if err != nil {
		t.Fatal(err.Error())
	}

	r, err := inst.NewRequest(http.MethodGet, "/cron/datastore-export", nil)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		b, _ := ioutil.ReadAll(w.Body)
		t.Fatalf("unexpected %d, expected 200, body=%s", w.Code, string(b))
	}

	if e, g := 1, len(mock.Values); e != g {
		t.Fatalf("unexpected Tasks.len expected %d; got %d", e, g)
	}

	var form TQDatastoreExportAPIPostRequest
	if err := json.Unmarshal(mock.Values[0].task.Payload, &form); err != nil {
		t.Fatal(err.Error())
	}
	if e, g := schedule.ProjectID, form.ProjectID; e != g {
		t.Fatalf("unexpected Tasks.ProjectID expected %s; got %s", e, g)
	}
	if e, g := schedule.Kinds[0], form.Kinds[0]; e != g {
		t.Fatalf("unexpected Tasks.Kinds[0] expected %s; got %s", e, g)
	}
	if e, g := schedule.Kinds[1], form.Kinds[1]; e != g {
		t.Fatalf("unexpected Tasks.Kinds[1] expected %s; got %s", e, g)
	}
	if e, g := schedule.Bucket, form.Bucket; e != g {
		t.Fatalf("unexpected Tasks.Bucket expected %s; got %s", e, g)
	}
}
