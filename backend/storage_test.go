package backend

import (
	"context"
	"fmt"
)

// MockStorageService is StorageServiceのMock実装
type MockStorageService struct {
	CallGetObjectCount int
	GetObjectResult    map[string]string
}

func NewMockStorageService() *MockStorageService {
	m := MockStorageService{
		GetObjectResult: map[string]string{},
	}

	return &m
}

func (service *MockStorageService) GetObject(ctx context.Context, bucket string, object string) (string, error) {
	service.CallGetObjectCount++

	v, ok := service.GetObjectResult[fmt.Sprintf("%s/%s", bucket, object)]
	if ok == false {
		return "", fmt.Errorf("gs://%s/%s is not found in Mock", bucket, object)
	}

	return v, nil
}

func (service *MockStorageService) AddMockGetObjectResult(bucket string, object string, result string) {
	service.GetObjectResult[fmt.Sprintf("%s/%s", bucket, object)] = result
}
