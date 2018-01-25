package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/favclip/testerator"
)

// MockDS2BQService is DS2BQService Mock
type MockDS2BQService struct {
	exportCallCount int
	form            *DatastoreExportForm
}

func (s *MockDS2BQService) Export(ctx context.Context, form *DatastoreExportForm) error {
	s.exportCallCount++
	s.form = form
	return nil
}

func TestTQDatastoreExportAPI_Post(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	mock := MockDS2BQService{}
	setDS2BQService(&mock)

	form := TQDatastoreExportAPIPostRequest{
		ProjectID: "sampleprojectid",
		Kinds:     []string{"hoge", "fuga"},
		Bucket:    "hoge",
	}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}

	r, err := inst.NewRequest(http.MethodPost, "/tq/datastore/export", bytes.NewReader(b))
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

	if e, g := 1, mock.exportCallCount; e != g {
		t.Fatalf("unexpected exportCallCount is %d. got : %d", e, g)
	}
	if e, g := form.ProjectID, mock.form.ProjectID; e != g {
		t.Fatalf("unexpected ProjectID is %s. got : %s", e, g)
	}
	if e, g := len(form.Kinds), len(mock.form.Kinds); e != g {
		t.Fatalf("unexpected Kinds.length is %d. got : %d", e, g)
	}
	if e, g := form.Kinds[0], mock.form.Kinds[0]; e != g {
		t.Fatalf("unexpected Kinds[0] is %s. got : %s", e, g)
	}
	if e, g := form.Kinds[1], mock.form.Kinds[1]; e != g {
		t.Fatalf("unexpected Kinds[1] is %s. got : %s", e, g)
	}
	if e, g := fmt.Sprintf("gs://%s", form.Bucket), mock.form.Bucket; e != g {
		t.Fatalf("unexpected Bucket is %s. got : %s", e, g)
	}
}
