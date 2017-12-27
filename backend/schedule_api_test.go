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

func TestScheduleAPI_Post(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	form := ScheduleAPIPostRequest{
		ProjectID:         "sampleprojectid",
		DstProjectID:      "dstprojectid",
		DstDatasetID:      "dstdatasetid",
		DstTableID:        "dsttableid",
		QueryPathBucket:   "hogebucket",
		QueryPathObject:   "hogeobject",
		CreateDisposition: "",
		ScheduleV1:        "every day 02:00",
		ScheduleTimezone:  "Asia/Tokyo",
	}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}

	r, err := inst.NewRequest(http.MethodPost, "/schedule", bytes.NewBuffer(b))
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
