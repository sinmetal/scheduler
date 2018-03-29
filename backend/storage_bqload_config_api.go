package backend

import (
	"context"
	"net/http"

	"github.com/favclip/ucon"
	"github.com/favclip/ucon/swagger"
)

// StorageBQLoadConfigAPI is Cloud StorageにUploadされたファイルをBigQueryにLoadするConfigに関するAPI
type StorageBQLoadConfigAPI struct{}

func setupStorageBQLoadConfigAPI(swPlugin *swagger.Plugin) {
	api := &StorageBQLoadConfigAPI{}

	tag := swPlugin.AddTag(&swagger.Tag{Name: "StorageBQLoadConfig", Description: "StorageBQLoadConfig list"})
	var hInfo *swagger.HandlerInfo

	hInfo = swagger.NewHandlerInfo(api.Post)
	ucon.Handle(http.MethodPost, "/storageBQLoadConfig", hInfo)
	hInfo.Description, hInfo.Tags = "post to StorageBQLoadConfig", []string{tag.Name}
}

// StorageBQLoadConfigAPIPostRequest is StorageBQLoadConfigAPI Post form
type StorageBQLoadConfigAPIPostRequest struct {
	FrmStorageBucket     string `json:"frmStorageBucket"`
	DstBigQueryProjectID string `json:"dstBigQueryProjectID"`
	DstBigQueryDataset   string `json:"dstBigQueryDataset"`
}

// StorageBQLoadConfigAPIPostResponse is StorageBQLoadConfigAPI Post response
type StorageBQLoadConfigAPIPostResponse struct {
	*StorageBQLoadConfig
}

// Post is Payloadの情報を元に StorageBQLoadConfig を新規でDatastoreに登録するAPI Handler
func (api *StorageBQLoadConfigAPI) Post(ctx context.Context, form *StorageBQLoadConfigAPIPostRequest) (*StorageBQLoadConfigAPIPostResponse, error) {
	store := StorageBQLoadConfigStore{}

	ds, err := fromContext(ctx)
	if err != nil {
		return nil, err
	}

	config := &StorageBQLoadConfig{
		FrmStorageBucket:     form.FrmStorageBucket,
		DstBigQueryProjectID: form.DstBigQueryProjectID,
		DstBigQueryDataset:   form.DstBigQueryDataset,
	}
	key := store.Key(ctx, ds, form.FrmStorageBucket)
	sc, err := store.Put(ctx, key, config)
	if err != nil {
		return nil, err
	}
	return &StorageBQLoadConfigAPIPostResponse{sc}, nil
}
