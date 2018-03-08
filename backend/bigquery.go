package backend

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/urlfetch"
)

// BigQueryService is BigQuery APIのためのService
type BigQueryService struct{}

// LoadFromCloudSQLExport is Cloud SQL Export CSVをBigQueryにLoadする
func (service *BigQueryService) LoadFromCloudSQLExport(ctx context.Context, job *ScheduleCloudSQLExportJob) error {
	log.Infof(ctx, "%v", job)

	client := &http.Client{
		Transport: &oauth2.Transport{
			Source: google.AppEngineTokenSource(ctx, bigquery.BigqueryScope),
			Base:   &urlfetch.Transport{Context: ctx},
		},
	}

	bqs, err := bigquery.New(client)
	if err != nil {
		return err
	}

	ts, err := service.BuildTableSchema(job.BigQueryTableSchema)
	if err != nil {
		return err
	}
	j := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: &bigquery.JobConfigurationLoad{
				SourceUris: []string{
					job.ExportURI,
				},
				DestinationTable: &bigquery.TableReference{
					ProjectId: job.BigQueryProjectID,
					DatasetId: job.BigQueryDataset,
					TableId:   job.BigQueryTable,
				},
				SourceFormat:     "CSV",
				Schema:           ts,
				WriteDisposition: "WRITE_TRUNCATE",
			},
		},
	}

	rj, err := bqs.Jobs.Insert(appengine.AppID(ctx), j).Do()
	if err != nil {
		log.Warningf(ctx, "unexpected error in BigQuery Load Job Insert: %s", err)
		return nil
	}
	log.Infof(ctx, "JobID=%s, Status=%s", rj.Id, rj.Status.State)

	return nil
}

// BuildTableSchema is TableSchema文字列からbigquery.TableSchemaを生成する
// TableSchema文字列 Example Name:STRING,Age:INTEGER
func (service *BigQueryService) BuildTableSchema(schema string) (*bigquery.TableSchema, error) {
	result := &bigquery.TableSchema{}
	s := strings.Split(schema, ",")
	for _, v := range s {
		fsc := strings.Split(v, ":")
		if service.validateTableFieldSchema(fsc) == false {
			return nil, fmt.Errorf("invalid TableFieldSchema. %v", fsc)
		}
		result.Fields = append(result.Fields,
			&bigquery.TableFieldSchema{
				Name: fsc[0],
				Type: fsc[1],
			})
	}

	return result, nil
}

// validateTableFieldSchema is TableSchema文字列がValidか確認する
// TableSchema文字列 Example Name:STRING,Age:INTEGER
func (service *BigQueryService) validateTableFieldSchema(fieldSchema []string) bool {
	if len(fieldSchema) != 2 {
		// 0 : Column Name, 1 : Type
		return false
	}
	// TODO TypeがBigQueryの許容範囲のものか確認する
	return true
}
