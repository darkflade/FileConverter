package eventtransport

import "fmt"

type Envelope struct {
	Nonce   string `json:"nonce"`
	Payload any    `json:"payload"`
}

type JobCreated struct {
	JobID string `json:"job_id"`
}

type Metadata struct {
	Filename string `json:"filename"`
}

type Progress struct {
	Percents float32 `json:"percents"`
}

// Errors---------------------------

type WSError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e WSError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func NewError(code, message string) WSError {
	return WSError{
		Code:    code,
		Message: message,
	}
}
