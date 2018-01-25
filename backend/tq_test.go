package backend

import (
	"context"

	"google.golang.org/appengine/taskqueue"
)

// MockTQService is TQ Service Mock
type MockTQService struct {
	Values []*TaskQueueValue
}

// TaskQueueValue is Task Addする時のセットValue
type TaskQueueValue struct {
	task      *taskqueue.Task
	queueName string
}

// Add is Task Queue Add Mock
func (s *MockTQService) Add(ctx context.Context, task *taskqueue.Task, queueName string) (*taskqueue.Task, error) {
	s.Values = append(s.Values, &TaskQueueValue{
		task,
		queueName,
	})
	return task, nil
}
