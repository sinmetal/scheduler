package backend

import (
	"context"
	"fmt"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

type storageService struct{}

// GetObject is Download from Cloud Storage Object
func (service *storageService) GetObject(ctx context.Context, bucket string, object string) (string, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("storage.NewClient: path=gs://%s/%s", bucket, object))
	}
	r, err := client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("storage.NewReader: path=gs://%s/%s", bucket, object))
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("ioutil.ReadAll: path=gs://%s/%s", bucket, object))
	}
	return string(b), nil
}
