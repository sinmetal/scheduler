package backend

import (
	"context"

	"go.mercari.io/datastore"
	"go.mercari.io/datastore/aedatastore"
)

func fromContext(ctx context.Context) (datastore.Client, error) {
	return aedatastore.FromContext(ctx)
}
