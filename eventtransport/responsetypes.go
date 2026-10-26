package eventtransport

import (
	"fmt"

	"github.com/google/uuid"
)

type EventMessagePayload interface {
	MessageName() string
}

type Envelope struct {
	Nonce   string
	Payload EventMessagePayload
}

type JobCreated struct {
	JobID string `json:"job_id"`
}

func (JobCreated) MessageName() string {
	return "job_created"
}

type Metadata struct {
	Filename string `json:"filename"`
}

func (Metadata) MessageName() string {
	return "metadata"
}

type Progress struct {
	Percent float64 `json:"percent"`
	State   string  `json:"state"`
}

func (Progress) MessageName() string {
	return "progress"
}

type JobFinished struct {
	JobID      uuid.UUID `json:"job_id"`
	OutputName string    `json:"output_name"`
	Size       int64     `json:"size"`
	MimeType   string    `json:"mime_type"`
}

func (JobFinished) MessageName() string {
	return "job_finished"
}

type JobFailed struct {
	JobID  uuid.UUID `json:"job_id"`
	Reason string    `json:"reason"`
}

func (JobFailed) MessageName() string {
	return "job_failed"
}

// Errors---------------------------

type EventError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (EventError) MessageName() string {
	return "error"
}

func (e EventError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func NewError(code, message string) EventError {
	return EventError{
		Code:    code,
		Message: message,
	}
}
