package backend

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/favclip/testerator"
)

var pubsubMessageBodySample = `{"message":{"data":"ew0KICAia2luZCI6ICJzdG9yYWdlI29iamVjdCIsDQogICJpZCI6ICJzdGFnaW5nLnNpbm1ldGFsLXNjaGVkdWxlci1kZXYuYXBwc3BvdC5jb20vR0NQVUctMTIucG5nLzE1MjM5NDgzMzI3NzM1NDkiLA0KICAic2VsZkxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vc3RvcmFnZS92MS9iL3N0YWdpbmcuc2lubWV0YWwtc2NoZWR1bGVyLWRldi5hcHBzcG90LmNvbS9vL0dDUFVHLTEyLnBuZyIsDQogICJuYW1lIjogIkdDUFVHLTEyLnBuZyIsDQogICJidWNrZXQiOiAic3RhZ2luZy5zaW5tZXRhbC1zY2hlZHVsZXItZGV2LmFwcHNwb3QuY29tIiwNCiAgImdlbmVyYXRpb24iOiAiMTUyMzk0ODMzMjc3MzU0OSIsDQogICJtZXRhZ2VuZXJhdGlvbiI6ICIxIiwNCiAgImNvbnRlbnRUeXBlIjogImltYWdlL3BuZyIsDQogICJ0aW1lQ3JlYXRlZCI6ICIyMDE4LTA0LTE3VDA2OjU4OjUyLjc3MFoiLA0KICAidXBkYXRlZCI6ICIyMDE4LTA0LTE3VDA2OjU4OjUyLjc3MFoiLA0KICAic3RvcmFnZUNsYXNzIjogIlNUQU5EQVJEIiwNCiAgInRpbWVTdG9yYWdlQ2xhc3NVcGRhdGVkIjogIjIwMTgtMDQtMTdUMDY6NTg6NTIuNzcwWiIsDQogICJzaXplIjogIjEzODA0MiIsDQogICJtZDVIYXNoIjogIjJFTmFzb3I4V3lodkNXVmlQN2t5WGc9PSIsDQogICJtZWRpYUxpbmsiOiAiaHR0cHM6Ly93d3cuZ29vZ2xlYXBpcy5jb20vZG93bmxvYWQvc3RvcmFnZS92MS9iL3N0YWdpbmcuc2lubWV0YWwtc2NoZWR1bGVyLWRldi5hcHBzcG90LmNvbS9vL0dDUFVHLTEyLnBuZz9nZW5lcmF0aW9uPTE1MjM5NDgzMzI3NzM1NDkmYWx0PW1lZGlhIiwNCiAgImNyYzMyYyI6ICJJS3R0K3c9PSIsDQogICJldGFnIjogIkNLM2huL2pkd05vQ0VBRT0iDQp9","attributes":{"bucketId":"staging.sinmetal-scheduler-dev.appspot.com","objectId":"GCPUG-12.png","objectGeneration":"1523948332773549","eventTime":"2018-04-17T06:58:52.770661Z","eventType":"OBJECT_FINALIZE","payloadFormat":"JSON_API_V1","notificationConfig":"projects/_/buckets/staging.sinmetal-scheduler-dev.appspot.com/notificationConfigs/1"},"message_id":"74768261217447","messageId":"74768261217447","publish_time":"2018-04-17T06:58:53.189Z","publishTime":"2018-04-17T06:58:53.189Z"},"subscription":"projects/sinmetal-scheduler-dev/subscriptions/sample"}`

func TestReceivePubSubSampleHandler(t *testing.T) {
	inst, _, err := testerator.SpinUp()
	if err != nil {
		t.Fatal(err.Error())
	}
	defer testerator.SpinDown()

	r, err := inst.NewRequest(http.MethodPost, "/_ah/push-handlers/sample", bytes.NewBuffer([]byte(pubsubMessageBodySample)))
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
