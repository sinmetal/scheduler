package backend

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/favclip/testerator"
)

func TestTQCloudSQLExportAfterAPI_Post(t *testing.T) {
	inst, ctx, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	ds, err := fromContext(ctx)
	if err != nil {
		t.Fatalf("%+v\n", err)
	}

	mock := MockCloudSQLAdminService{}
	SetCloudSQLAdminService(&mock)

	store := ScheduleCloudSQLExportJobStore{}
	jobKey := store.Key(ctx, ds, "gs://hoge/fuga.csv")

	const projectID = "hogepro"
	const instance = "hogein"

	mustore := CloudSQLExportMutexStore{}
	if err := mustore.Lock(ctx, projectID, instance, jobKey); err != nil {
		t.Fatal(err)
	}

	form := TQCloudSQLExportAPIAfterPostRequest{
		ProjectID: projectID,
		Instance:  instance,
		JobKey:    jobKey.Encode(),
	}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}

	r, err := inst.NewRequest(http.MethodPost, "/tq/cloudsql/export/after", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json;charset=utf-8")

	w := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		b, _ := ioutil.ReadAll(w.Body)
		t.Fatalf("unexpected %d, expected 200, body=%s", w.Code, string(b))
	}

	if e, g := 1, mock.CallGetOpCount; e != g {
		t.Fatalf("expected CallGetOpCount is %d; got %d", e, g)
	}
}
