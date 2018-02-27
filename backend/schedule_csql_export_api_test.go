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

// TestScheduleCloudSQLExportAPI_Post
// とりあえず、 200 OKが返ってくるのを確認
func TestScheduleCloudSQLExportAPI_Post(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	form := ScheduleCloudSQLExportAPIPostRequest{
		ProjectID: "sampleprojectid",
		Databases: []string{"db1"},
		SQLURI:    "gs://hoge/sample.sql",
		ExportURI: "gs://hoge/%s/sample.csv",
	}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}

	r, err := inst.NewRequest(http.MethodPost, "/scheduleCloudSQLExport", bytes.NewBuffer(b))
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
}
