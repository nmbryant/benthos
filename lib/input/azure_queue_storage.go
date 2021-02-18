// +build !wasm

package input

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/service/azure"

	"github.com/Azure/azure-storage-queue-go/azqueue"
	"github.com/Jeffail/benthos/v3/lib/message"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// AzureQueueStorage is a benthos reader.Type implementation that reads messages
// from an Azure Queue Storage container.
type azureQueueStorage struct {
	conf AzureQueueStorageConfig

	queueURL *azqueue.QueueURL

	log   log.Modular
	stats metrics.Type
}

// newAzureQueueStorage creates a new Azure Storage Queue input type.
func newAzureQueueStorage(conf AzureQueueStorageConfig, log log.Modular, stats metrics.Type) (*azureQueueStorage, error) {
	serviceURL, err := azure.GetQueueServiceURL(conf.StorageAccount, conf.StorageAccessKey, conf.StorageConnectionString)
	if err != nil {
		return nil, err
	}
	queueURL := serviceURL.NewQueueURL(conf.QueueName)
	a := &azureQueueStorage{
		conf:     conf,
		log:      log,
		stats:    stats,
		queueURL: &queueURL,
	}
	return a, nil
}

// ConnectWithContext attempts to establish a connection
func (a *azureQueueStorage) ConnectWithContext(ctx context.Context) error {
	return nil
}

// ReadWithContext attempts to read a new message from the target Azure Storage Queue
// Storage container.
func (a *azureQueueStorage) ReadWithContext(ctx context.Context) (msg types.Message, ackFn reader.AsyncAckFn, err error) {
	messageURL := a.queueURL.NewMessagesURL()
	dequeue, err := messageURL.Dequeue(ctx, 1, 30*time.Second)
	if err != nil {
		if cerr, ok := err.(azqueue.StorageError); ok {
			if cerr.ServiceCode() == azqueue.ServiceCodeQueueNotFound {
				ctx := context.Background()
				_, err = a.queueURL.Create(ctx, azqueue.Metadata{})
				return nil, nil, err
			}
			return nil, nil, fmt.Errorf("storage error message: %v", cerr)
		}
		return nil, nil, fmt.Errorf("error dequeing message: %v", err)
	}
	if n := dequeue.NumMessages(); n > 0 {
		props, _ := a.queueURL.GetProperties(ctx)
		metadata := props.NewMetadata()
		msg := message.New(nil)
		for m := int32(0); m < dequeue.NumMessages(); m++ {
			queueMsg := dequeue.Message(m)
			part := message.NewPart([]byte(queueMsg.Text))
			msg.Append(part)
			meta := msg.Get(0).Metadata()
			meta.Set("queue_storage_insertion_time", queueMsg.InsertionTime.Format(time.RFC3339))
			for k, v := range metadata {
				meta.Set(k, v)
			}
			msgIDURL := messageURL.NewMessageIDURL(queueMsg.ID)
			_, err = msgIDURL.Delete(ctx, queueMsg.PopReceipt)
		}
		return msg, func(rctx context.Context, res types.Response) error {
			return nil
		}, nil
	}
	return nil, nil, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *azureQueueStorage) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *azureQueueStorage) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
