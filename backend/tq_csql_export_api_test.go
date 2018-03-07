package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/favclip/testerator"
)

type MockCloudSQLAdminService struct {
	CallExportCount int
	Config          *CloudSQLExportConfig
}

func (s *MockCloudSQLAdminService) Export(ctx context.Context, form *CloudSQLExportConfig) error {
	s.CallExportCount++
	s.Config = form
	return nil
}

func TestTQCloudSQLExportAPI_Post(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	const bucket = "hogebucket"
	const object = "hogeobject"
	const sql = "select * from sample"

	ms := NewMockStorageService()
	ms.AddMockGetObjectResult(bucket, object, sql)
	SetStorageService(ms)

	mock := MockCloudSQLAdminService{}
	SetCloudSQLAdminService(&mock)

	form := TQCloudSQLExportAPIPostRequest{
		ProjectID:           "hogeproject",
		Instance:            "sql1",
		Databases:           []string{"db1"},
		SQLBucket:           bucket,
		SQLObject:           object,
		ExportURI:           "gs://hoge/%s/hoge.csv",
		BigQueryProjectID:   "hogeproject",
		BigQueryDataset:     "hogedataset",
		BigQueryTable:       "hogetable",
		BigQueryTableSchema: "NAME:STRING,AGE:INTEGER",
	}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}

	r, err := inst.NewRequest(http.MethodPost, "/tq/cloudsql/export", bytes.NewReader(b))
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

	if e, g := 1, mock.CallExportCount; e != g {
		t.Fatalf("expected CallExportCount is %d; got %d", e, g)
	}
	if e, g := mock.Config.ProjectID, form.ProjectID; e != g {
		t.Fatalf("expected ProjectID is %s; got %s", e, g)
	}
	if e, g := mock.Config.Instance, form.Instance; e != g {
		t.Fatalf("expected Instance is %s; got %s", e, g)
	}
	if e, g := mock.Config.Databases[0], form.Databases[0]; e != g {
		t.Fatalf("expected Databases[0] is %s; got %s", e, g)
	}
	if e, g := mock.Config.SQL, sql; e != g {
		t.Fatalf("expected SQL is %s; got %s", e, g)
	}

	// TODO ScheduleCloudSQLExportJobのTest. time.Nowを固定していないので、yyyymmddhhmmssが分からない

	// TODO time.Nowを固定していないので、yyyymmddhhmmssが入るところを固定できない
	//if e, g := mock.Config.ExportURI, form.ExportURI; e != g {
	//	t.Fatalf("expected ExportURI is %s; got %s", e, g)
	//}
}
