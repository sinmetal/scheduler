// Code generated by "stringer -type PubSubStorageNotifyEventType pubsub_storage_notify_event_type.go"; DO NOT EDIT.

package backend

import "fmt"

const _PubSubStorageNotifyEventType_name = "ObjectFinalizeObjectMetaDataUpdateObjectDeleteObjectArchive"

var _PubSubStorageNotifyEventType_index = [...]uint8{0, 14, 34, 46, 59}

func (i PubSubStorageNotifyEventType) String() string {
	if i < 0 || i >= PubSubStorageNotifyEventType(len(_PubSubStorageNotifyEventType_index)-1) {
		return fmt.Sprintf("PubSubStorageNotifyEventType(%d)", i)
	}
	return _PubSubStorageNotifyEventType_name[_PubSubStorageNotifyEventType_index[i]:_PubSubStorageNotifyEventType_index[i+1]]
}