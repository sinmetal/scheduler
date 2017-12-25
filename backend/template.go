package backend

import (
	"bytes"
	"text/template"
)

// ExecuteTemplate is text template execute
func ExecuteTemplate(query string, v map[string]interface{}) (string, error) {
	buf := new(bytes.Buffer)
	t := template.Must(template.New("query").Parse(query))
	err := t.Execute(buf, v)
	if err != nil {
		return "", err
	}

	return string(buf.Bytes()), nil
}
