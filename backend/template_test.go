package backend

import "testing"

func TestExecuteTemplate(t *testing.T) {
	query := "SELECT * FROM hoge{{.DATE}}"
	text, err := ExecuteTemplate(query, map[string]interface{}{
		"DATE": "20171111",
	})
	if err != nil {
		t.Fatal(err)
	}
	if e, g := "SELECT * FROM hoge20171111", text; e != g {
		t.Fatalf("expected text: %s; got %s", e, g)
	}
}
