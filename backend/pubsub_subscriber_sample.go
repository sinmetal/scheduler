package backend

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/appengine/log"
)

// ReceivePubSubSampleHandler is Cloud Pus/SubのPushを受け取って解釈するHandler
func ReceivePubSubSampleHandler(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	for k, v := range r.Header {
		log.Infof(ctx, "%s:%s", k, v)
	}
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Errorf(ctx, "%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	msg, err := ReadPubSubBody(body)
	if err != nil {
		log.Errorf(ctx, "%+v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Infof(ctx, "%+v", msg)
	w.WriteHeader(http.StatusOK)
}

// PubSubBody is PubSubからPushされたMessageのBody
type PubSubBody struct {
	Message      PubSubMessage `json:"message"`
	Subscription string        `json:"subscription"`
}

type pubSubBody struct {
	Message      pubSubMessage `json:"message"`
	Subscription string        `json:"subscription"`
}

// PubSubMessage is PubSubからPushされたMessageの中で、Messageに関連すること
type PubSubMessage struct {
	Data        PubSubMessageData `json:"data"`
	Attributes  PubSubAttributes  `json:"attributes"`
	MessageID   string            `json:"messageId"`
	PublishTime time.Time         `json:"publishTime"`
}

type pubSubMessage struct {
	Data        string           `json:"data"`
	Attributes  pubSubAttributes `json:"attributes"`
	MessageID   string           `json:"messageId"`
	PublishTime time.Time        `json:"publishTime"`
}

type pubSubMessageData struct {
	Kind                    string    `json:"kind"`
	ID                      string    `json:"id"`
	SelfLink                string    `json:"selfLink"`
	Name                    string    `json:"name"`
	Bucket                  string    `json:"bucket"`
	Generation              string    `json:"generation"`
	Metageneration          string    `json:"metageneration"`
	ContentType             string    `json:"contentType"`
	TimeCreated             time.Time `json:"timeCreated"`
	Updated                 time.Time `json:"updated"`
	StorageClass            string    `json:"storageClass"`
	TimeStorageClassUpdated time.Time `json:"timeStorageClassUpdated"`
	Size                    string    `json:"size"`
	MD5Hash                 string    `json:"md5hash"`
	MediaLink               string    `json:"mediaLink"`
	CRC32C                  string    `json:"crc32c"`
	Etag                    string    `json:"etag"`
}

// PubSubMessageData is PubSubからPushされたMessageのObjectに関連する内容
type PubSubMessageData struct {
	Kind                    string           `json:"kind"`
	ID                      string           `json:"id"`
	SelfLink                string           `json:"selfLink"`
	Name                    string           `json:"name"`
	Bucket                  string           `json:"bucket"`
	Generation              int              `json:"generation"`
	Metageneration          int              `json:"metageneration"`
	ContentType             string           `json:"contentType"`
	TimeCreated             time.Time        `json:"timeCreated"`
	Updated                 time.Time        `json:"updated"`
	StorageClass            StorageClassType `json:"storageClass"`
	TimeStorageClassUpdated time.Time        `json:"timeStorageClassUpdated"`
	Size                    int              `json:"size"`
	MD5Hash                 string           `json:"md5hash"`
	MediaLink               string           `json:"mediaLink"`
	CRC32C                  string           `json:"crc32c"`
	Etag                    string           `json:"etag"`
}

// PubSubAttributes is PubSubからPushされたMessageのObjectの変更に関連する内容
type PubSubAttributes struct {
	BucketID           string                       `json:"bucketId"`
	ObjectID           string                       `json:"objectId"`
	ObjectGeneration   string                       `json:"objectGeneration"`
	EventTime          time.Time                    `json:"eventTime"`
	EventType          PubSubStorageNotifyEventType `json:"eventType"`
	PayloadFormat      string                       `json:"payloadFormat"`
	NotificationConfig string                       `json:"notificationConfig"`
}

type pubSubAttributes struct {
	BucketID           string    `json:"bucketId"`
	ObjectID           string    `json:"objectId"`
	ObjectGeneration   string    `json:"objectGeneration"`
	EventTime          time.Time `json:"eventTime"`
	EventType          string    `json:"eventType"`
	PayloadFormat      string    `json:"payloadFormat"`
	NotificationConfig string    `json:"notificationConfig"`
}

// ReadPubSubBody is PubSubからPushされたリクエストのBodyを読み込む
func ReadPubSubBody(body []byte) (*PubSubBody, error) {
	var b pubSubBody
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, err
	}

	r := base64.NewDecoder(base64.StdEncoding, strings.NewReader(b.Message.Data))
	d, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var md pubSubMessageData
	if err := json.Unmarshal(d, &md); err != nil {
		return nil, err
	}
	psmd := PubSubMessageData{}
	psmd.Kind = md.Kind
	psmd.ID = md.ID
	psmd.SelfLink = md.SelfLink
	psmd.Name = md.Name
	psmd.Bucket = md.Bucket
	psmd.ContentType = md.ContentType
	psmd.TimeCreated = md.TimeCreated
	psmd.Updated = md.Updated
	psmd.TimeStorageClassUpdated = md.TimeStorageClassUpdated
	psmd.MD5Hash = md.MD5Hash
	psmd.MediaLink = md.MediaLink
	psmd.CRC32C = md.CRC32C
	psmd.Etag = md.Etag

	sct, err := ParseStorageClassType(md.StorageClass)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed ParseStorageClassType. v=%s", md.StorageClass))
	}
	psmd.StorageClass = sct

	size, err := strconv.Atoi(md.Size)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed Size ParseInt. Size = %s", md.Size))
	}
	psmd.Size = size

	g, err := strconv.Atoi(md.Generation)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed Generation Atoi. Generation = %s", md.Generation))
	}
	psmd.Generation = g

	mg, err := strconv.Atoi(md.Metageneration)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed Metageneration Atoi. Metageneration = %s", md.Metageneration))
	}
	psmd.Metageneration = mg

	a := PubSubAttributes{
		BucketID:           b.Message.Attributes.BucketID,
		ObjectID:           b.Message.Attributes.ObjectID,
		ObjectGeneration:   b.Message.Attributes.ObjectGeneration,
		EventTime:          b.Message.Attributes.EventTime,
		PayloadFormat:      b.Message.Attributes.PayloadFormat,
		NotificationConfig: b.Message.Attributes.NotificationConfig,
	}
	et, err := ParsePubSubStorageNotifyEventType(b.Message.Attributes.EventType)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed ParsePubSubStorageNotifyEventType. v=%s", b.Message.Attributes.EventType))
	}
	a.EventType = et

	return &PubSubBody{
		Message: PubSubMessage{
			Data:        psmd,
			Attributes:  a,
			MessageID:   b.Message.MessageID,
			PublishTime: b.Message.PublishTime,
		},
		Subscription: b.Subscription,
	}, nil
}
