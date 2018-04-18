package backend

import "errors"

// PubSubStorageNotifyEventType is Cloud Storage PubSub Notification EventType
// see https://cloud.google.com/storage/docs/pubsub-notifications#events
type PubSubStorageNotifyEventType int

// PubSubStorageNotifyEventType
const (
	ObjectFinalize PubSubStorageNotifyEventType = iota
	ObjectMetaDataUpdate
	ObjectDelete
	ObjectArchive
)

// ErrParseFailure is Parse失敗時のError
var ErrParseFailure = errors.New("parse fail")

// ParsePubSubStorageNotifyEventType is 文字列から PubSubStorageNotifyEventType へ変換する
func ParsePubSubStorageNotifyEventType(eventType string) (PubSubStorageNotifyEventType, error) {
	switch eventType {
	case "OBJECT_FINALIZE":
		return ObjectFinalize, nil
	case "OBJECT_METADATA_UPDATE":
		return ObjectMetaDataUpdate, nil
	case "OBJECT_DELETE":
		return ObjectDelete, nil
	case "OBJECT_ARCHIVE":
		return ObjectArchive, nil
	default:
		return -1, ErrParseFailure
	}
}
