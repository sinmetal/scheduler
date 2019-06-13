package backend

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/log"
)

// BigQueryService is BigQuery APIのためのService
type BigQueryService struct{}

// LoadFromCloudSQLExport is Cloud SQL Export CSVをBigQueryにLoadする
func (service *BigQueryService) LoadFromCloudSQLExport(ctx context.Context, job *ScheduleCloudSQLExportJob) error {
	log.Infof(ctx, "%v", job)

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

	_, err = service.insertLoadJob(ctx, j)
	if err != nil {
		return err
	}

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

// BigQueryLoadWithAutodetectParam is LoadWithAutodetect Param
type BigQueryLoadWithAutodetectParam struct {
	CloudStorageURI   string
	BigQueryProjectID string
	BigQueryDataset   string
	BigQueryTable     string
	SourceFormat      string
}

// LoadWithAutodetect is BigQuery.LoadをAutodetectで実行する
func (service *BigQueryService) LoadWithAutodetect(ctx context.Context, param *BigQueryLoadWithAutodetectParam) error {
	j := &bigquery.Job{
		Configuration: &bigquery.JobConfiguration{
			Load: &bigquery.JobConfigurationLoad{
				SourceUris: []string{
					param.CloudStorageURI,
				},
				DestinationTable: &bigquery.TableReference{
					ProjectId: param.BigQueryProjectID,
					DatasetId: param.BigQueryDataset,
					TableId:   param.BigQueryTable,
				},
				SourceFormat:     param.SourceFormat,
				Autodetect:       true,
				WriteDisposition: "WRITE_TRUNCATE",
			},
		},
	}

	_, err := service.insertLoadJob(ctx, j)
	if err != nil {
		return err
	}

	return nil
}

func (service *BigQueryService) insertLoadJob(ctx context.Context, j *bigquery.Job) (*bigquery.Job, error) {
	client, err := google.DefaultClient(ctx, bigquery.BigqueryScope)
	if err != nil {
		return nil, errors.Wrap(err, "failed google.DefaultClient")
	}

	bqs, err := bigquery.New(client)
	if err != nil {
		return nil, errors.Wrap(err, "failed bigquery.New")
	}

	rj, err := bqs.Jobs.Insert(appengine.AppID(ctx), j).Do()
	if err != nil {
		return nil, errors.Wrap(err, "failed bigquery.Jobs.Insert")
	}
	log.Infof(ctx, "JobID=%s, Status=%s", rj.Id, rj.Status.State)
	return rj, nil
}
