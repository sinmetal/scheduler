package backend

import (
	"context"

	"google.golang.org/api/sqladmin/v1beta4"
)

type MockCloudSQLAdminService struct {
	CallExportCount int
	CallGetOpCount  int
	Config          *CloudSQLExportConfig
}

func (s *MockCloudSQLAdminService) Export(ctx context.Context, form *CloudSQLExportConfig) (*sqladmin.Operation, error) {
	s.CallExportCount++
	s.Config = form
	return &sqladmin.Operation{Name: "hogename"}, nil
}

func (s *MockCloudSQLAdminService) GetOp(ctx context.Context, projectID string, operation string) (*sqladmin.Operation, error) {
	s.CallGetOpCount++
	return &sqladmin.Operation{Name: "hogename", Status: "DONE"}, nil
}
