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

func TestReceiveOCNHandler(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	form := struct{}{}
	b, err := json.Marshal(form)
	if err != nil {
		t.Fatal(err)
	}

	r, err := inst.NewRequest(http.MethodPost, "/ocn/datastore-export", bytes.NewBuffer(b))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json;charset=utf-8")

	w := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, r)

	// とりあえず、HandlerにリクエストがいってればOK
	if w.Code == http.StatusNotFound {
		b, _ := ioutil.ReadAll(w.Body)
		t.Fatalf("unexpected %d, expected 404, body=%s", w.Code, string(b))
	}
}
