package ds2bq

import (
	"fmt"
	"context"
	"net/http"

	"github.com/pkg/errors"
	"google.golang.org/appengine/log"
	"golang.org/x/oauth2/google"
	dsapi "google.golang.org/api/datastore/v1beta1"
)

// DS2BQService is Datastore to BigQuery Service
type DS2BQService interface {
	Export(ctx context.Context, form *ExportForm) error
}

// DS2BQServiceImple is Default DS2BQService
type DS2BQServiceImple struct{}

var ds2bqService *DS2BQService

// NewDS2BQService is New DS2BQService
func NewDS2BQService() DS2BQService {
	if ds2bqService != nil {
		return *ds2bqService
	}
	return &DS2BQServiceImple{}
}

// SetDS2BQService is Replace Imple
// Use it to replace with Mock with Unit Test.
func SetDS2BQService(service DS2BQService) {
	ds2bqService = &service
}

// ExportForm is Export Request Param
type ExportForm struct {
	ProjectID string
	Bucket string
	Kinds []string
}

// Export is Datastore Export to Cloud Storage Job Insert
func (s *DS2BQServiceImple) Export(ctx context.Context, form *ExportForm) error {
	client, err := google.DefaultClient(ctx, dsapi.DatastoreScope)
	if err != nil {
		return err
	}

	service, err := dsapi.New(client)
	if err != nil {
		return err
	}

	eCall := service.Projects.Export(form.ProjectID, &dsapi.GoogleDatastoreAdminV1beta1ExportEntitiesRequest{
		EntityFilter: &dsapi.GoogleDatastoreAdminV1beta1EntityFilter{
			Kinds:           form.Kinds,
			NamespaceIds:    []string{},
			ForceSendFields: []string{},
			NullFields:      []string{},
		},
		OutputUrlPrefix: fmt.Sprintf("gs://%s", form.Bucket),
	})
	op, err := eCall.Do()
	if err != nil {
		return errors.Wrap(err, "failed datastore export")
	}
	log.Infof(ctx, "Datastore Export Reponse Status Code : %s", op.HTTPStatusCode)
	rj, err := op.Response.MarshalJSON()
	if err != nil {
		log.Errorf(ctx, "failed datastore export response to marshalJSON: %v", err)
		return err
	}
	log.Infof(ctx, "%s", string(rj))

	if (op.HTTPStatusCode != http.StatusOK) {
		return fmt.Errorf("Datastore Export Response Status Code = %d", op.HTTPStatusCode)
	}

	return nil
}