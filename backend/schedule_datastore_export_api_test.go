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

// TestScheduleDatastoreExportAPI_Post
// とりあえず、200 OKが返ってくるのを確認
func TestScheduleDatastoreExportAPI_Post(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	form := ScheduleDatastoreExportAPIPostRequest{
		ProjectID: "sampleprojectid",
		Kinds:     []string{"hoge", "fuga"},
		Bucket:    "hoge",
	}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}

	r, err := inst.NewRequest(http.MethodPost, "/scheduleDatastoreExport", bytes.NewBuffer(b))
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		b, _ := ioutil.ReadAll(w.Body)
		t.Fatalf("unexpected %d, expected 200, body=%s", w.Code, string(b))
	}
}
