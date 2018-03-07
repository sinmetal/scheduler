package backend

import (
	"context"
	"fmt"
	"net/http"

	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sqladmin/v1beta4"
	"google.golang.org/appengine/log"
)

// CloudSQLAdminService is Cloud SQLのAdmin APIに関するService
type CloudSQLAdminService interface {
	Export(ctx context.Context, config *CloudSQLExportConfig) error
}

// CloudSQLAdminServiceImpl is Cloud SQL Adminの実装をぶらさげるstruct
type CloudSQLAdminServiceImpl struct{}

var cloudsqlService *CloudSQLAdminService

// NewCloudSQLAdminService is CloudSQLAdminServiceを作成
func NewCloudSQLAdminService() CloudSQLAdminService {
	if cloudsqlService != nil {
		return *cloudsqlService
	}
	return &CloudSQLAdminServiceImpl{}
}

// SetCloudSQLAdminService is Replace Imple
// Use it to replace with Mock with Unit Test.
func SetCloudSQLAdminService(service CloudSQLAdminService) {
	cloudsqlService = &service
}

// CloudSQLExportConfig is Cloud SQL Export API実行に必要なパラメータ
type CloudSQLExportConfig struct {
	ProjectID string
	Instance  string
	Databases []string
	SQL       string
	ExportURI string
}

// Export is Cloud SQLにSQLを実行して、Cloud StorageにExportする
func (service *CloudSQLAdminServiceImpl) Export(ctx context.Context, config *CloudSQLExportConfig) error {
	client, err := google.DefaultClient(ctx, "https://www.googleapis.com/auth/cloud-platform", sqladmin.SqlserviceAdminScope)
	if err != nil {
		return err
	}

	admin, err := sqladmin.New(client)
	if err != nil {
		return errors.Wrap(err, "failed sqladmin.New")
	}

	param := sqladmin.InstancesExportRequest{
		ExportContext: &sqladmin.ExportContext{
			Uri:       config.ExportURI,
			FileType:  "CSV",
			Databases: config.Databases,
			CsvExportOptions: &sqladmin.ExportContextCsvExportOptions{
				SelectQuery: config.SQL,
			},
		},
	}
	op, err := admin.Instances.Export(config.ProjectID, config.Instance, &param).Do()
	if err != nil {
		return errors.Wrap(err, "failed sqladmin.Instances.Export")
	}

	log.Infof(ctx, "Cloud SQL Export Response Status Code : %d, Name : %s", op.HTTPStatusCode, op.Name)
	if op.HTTPStatusCode != http.StatusOK {
		return fmt.Errorf("Cloud SQL Export Response Status Code = %d", op.HTTPStatusCode)
	}

	return nil
}
