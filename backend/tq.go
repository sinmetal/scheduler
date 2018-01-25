package backend

import (
	"context"

	"github.com/pkg/errors"
	"google.golang.org/appengine/taskqueue"
)

// TaskQueueService is TaskQueueService Interface
type TaskQueueService interface {
	Add(ctx context.Context, task *taskqueue.Task, queueName string) (*taskqueue.Task, error)
}

// TaskQueueServiceImpl is Default TaskQueueService
type TaskQueueServiceImpl struct{}

var taskqueueService *TaskQueueService

// NewTaskQueueService is New TaskQueueService
func NewTaskQueueService() TaskQueueService {
	if taskqueueService != nil {
		return *taskqueueService
	}
	return &TaskQueueServiceImpl{}
}

// SetTaskQueueService is Replace Imple
// Use it to replace with Mock with Unit Test.
func SetTaskQueueService(service TaskQueueService) {
	taskqueueService = &service
}

// Add is Task add to queue
func (s *TaskQueueServiceImpl) Add(ctx context.Context, task *taskqueue.Task, queueName string) (*taskqueue.Task, error) {
	t, err := taskqueue.Add(ctx, task, queueName)
	if err != nil {
		return nil, errors.Wrap(err, "failed taskqueue.add")
	}
	return t, nil
}
