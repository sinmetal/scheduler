package backend

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"encoding/json"
	"github.com/favclip/testerator"
)

func TestCronCloudSQLExportAPI_Get(t *testing.T) {
	inst, ctx, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	mock := MockTQService{}
	SetTaskQueueService(&mock)

	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Scheduleとして1件登録しておく
	store := ScheduleCloudSQLExportStore{}
	key := store.NewKey(ctx, ds)
	schedule := &ScheduleCloudSQLExport{
		ProjectID: "hogeproject",
		Instance:  "sql1",
		Databases: []string{"db1"},
		SQLBucket: "hogebucket",
		SQLObject: "hogeobject.sql",
		ExportURI: "gs://hoge/%s/fuga.csv",
	}
	_, err = store.Put(ctx, key, schedule)
	if err != nil {
		t.Fatal(err.Error())
	}

	r, err := inst.NewRequest(http.MethodGet, "/cron/cloudsql-export", nil)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		b, _ := ioutil.ReadAll(w.Body)
		t.Fatalf("unexpected %d, expected 200, body=%s", w.Code, string(b))
	}

	var form TQCloudSQLExportAPIPostRequest
	if err := json.Unmarshal(mock.Values[0].task.Payload, &form); err != nil {
		t.Fatal(err)
	}
	if e, g := 1, len(mock.Values); e != g {
		t.Fatalf("unexpected Tasks.len expected %d; got %d", e, g)
	}
	if e, g := "/tq/cloudsql/export", mock.Values[0].task.Path; e != g {
		t.Fatalf("unexpected Tasks.Path expected %s; got %s", e, g)
	}
	if e, g := schedule.ProjectID, form.ProjectID; e != g {
		t.Fatalf("unexpected Tasks.ProjectID expected %s; got %s", e, g)
	}
	if e, g := schedule.Instance, form.Instance; e != g {
		t.Fatalf("unexpected Tasks.Instance expected %s; got %s", e, g)
	}
	if e, g := len(schedule.Databases), len(form.Databases); e != g {
		t.Fatalf("unexpected Tasks.Databases.length expected %d; got %d", e, g)
	}
	if e, g := schedule.SQLBucket, form.SQLBucket; e != g {
		t.Fatalf("unexpected Tasks.SQLBucket expected %s; got %s", e, g)
	}
	if e, g := schedule.SQLObject, form.SQLObject; e != g {
		t.Fatalf("unexpected Tasks.SQLObject expected %s; got %s", e, g)
	}

	// TODO time.Nowが固定されてないので、テストでひっかかる
	//if e, g := schedule.ExportURI, form.ExportURI; e != g {
	//	t.Fatalf("unexpected Tasks.ExportURI expected %s; got %s", e, g)
	//}
}
