package backend

import "testing"

func TestBigQueryService_BuildTableSchema(t *testing.T) {
	s := BigQueryService{}

	stext := "id:INTEGER,uuid:STRING,client_time:DATETIME,db_time:DATETIME"
	ts, err := s.BuildTableSchema(stext)
	if err != nil {
		t.Fatal(err)
	}
	if e, g := 4, len(ts.Fields); e != g {
		t.Fatalf("expected Fileds.length %d; got %d", e, g)
	}

	candidates := []struct {
		name       string
		schemaType      string
	}{
		{"id", "INTEGER"},
		{"uuid","STRING"},
		{"client_time","DATETIME"},
		{"db_time","DATETIME"},
	}

	for i, v := range candidates {
		if e, g := v.name, ts.Fields[i].Name; e != g {
			t.Fatalf("i = %d expected Field Name %s; got %s", i, e, g)
		}
		if e, g := v.schemaType, ts.Fields[i].Type; e != g {
			t.Fatalf("i = %d expected Field Type %s; got %s", i, e, g)
		}
	}
}
