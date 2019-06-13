package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
)

// ImportBigQueryHandleFunc returns a http.HandlerFunc that imports GCSObject to BigQuery.
func ImportBigQueryHandleFunc(datasetID string) http.HandlerFunc {
	// TODO: processWithContext
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := appengine.NewContext(r)

		req, err := DecodeGCSObjectToBQJobReq(r.Body)
		if err != nil {
			log.Errorf(ctx, "ds2bq: failed to decode request: %s", err)
			return
		}
		defer r.Body.Close()

		store := ScheduleDatastoreExportStore{}
		sl, err := store.QueryByBucket(ctx, req.Bucket)
		if err != nil {
			log.Errorf(ctx, "Datastore: QueryByBucket: Buckdet = %s; %v", req.Bucket, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if len(sl) != 1 {
			log.Errorf(ctx, "Query Results ScheduleDatastoreExport.length != 1; got %d; bucket %s", len(sl), req.Bucket)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = insertImportJob(ctx, req, sl[0].ProjectID, datasetID)
		if err != nil {
			log.Errorf(ctx, "ds2bq: failed to import BigQuery: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
}

// DecodeGCSObjectToBQJobReq decodes a GCSObjectToBQJobReq from r.
func DecodeGCSObjectToBQJobReq(r io.Reader) (*GCSObjectToBQJobReq, error) {
	decoder := json.NewDecoder(r)
	var req *GCSObjectToBQJobReq
	err := decoder.Decode(&req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

// GCSObjectToBQJobReq means request of OCN to BQ.
type GCSObjectToBQJobReq struct {
	Bucket      string    `json:"bucket"`
	FilePath    string    `json:"filePath"`
	KindName    string    `json:"kindName"`
	TimeCreated time.Time `json:"TimeCreated"`
}

func insertImportJob(ctx context.Context, req *GCSObjectToBQJobReq, projectID, datasetID string) error {
	log.Infof(ctx, "ds2bq: bucket: %s, filePath: %s, timeCreated: %s", req.Bucket, req.FilePath, req.TimeCreated)

	if req.Bucket == "" || req.FilePath == "" || req.KindName == "" {
		log.Warningf(ctx, "ds2bq: unexpected parameters %#v", req)
		return nil
	}

	client, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return err
	}

	bqs, err := bigquery.New(client)
	if err != nil {
		return err
	}

	job := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: &bigquery.JobConfigurationLoad{
				SourceUris: []string{
					fmt.Sprintf("gs://%s/%s", req.Bucket, req.FilePath),
				},
				DestinationTable: &bigquery.TableReference{
					ProjectId: projectID,
					DatasetId: datasetID,
					TableId:   req.KindName,
				},
				SourceFormat:     "DATASTORE_BACKUP",
				WriteDisposition: "WRITE_TRUNCATE",
			},
		},
	}

	rj, err := bqs.Jobs.Insert(appengine.AppID(ctx), job).Do()
	if err != nil {
		log.Warningf(ctx, "ds2bq: unexpected error in HandleBackupToBQJob: %s", err)
		return nil
	}
	log.Infof(ctx, "JobID=%s, Status=%s", rj.Id, rj.Status.State)

	return nil
}
