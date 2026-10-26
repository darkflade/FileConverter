package mediaconverter

import (
	"FileConverter/config"
	"FileConverter/eventtransport"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
)

var (
	ProcessingQueue = make(chan uuid.UUID, 100)
	Jobs            = NewJobStore()
)

type ConversionJob struct {
	ID           uuid.UUID
	UserID       uuid.UUID
	InputName    string
	TargetFormat string
	InputPath    string
	OutputPath   string
	Status       JobStatus
	Error        string
	Events       chan JobEvent
	//TODO change to private
	Done chan struct{}
	once sync.Once
	mu   sync.RWMutex
}

type JobEvent struct {
	Status JobStatus
	Error  string
}

type JobStatus int

const (
	JobStatusCreated JobStatus = iota
	JobStatusUploading
	JobStatusUploaded
	JobStatusProcessing
	JobStatusDone
	JobStatusFailed
)

type JobStore struct {
	mu   sync.RWMutex
	jobs map[uuid.UUID]*ConversionJob
}

func NewJobStore() *JobStore {
	return &JobStore{jobs: make(map[uuid.UUID]*ConversionJob)}
}

func (s *JobStore) Add(job *ConversionJob) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
}

func (s *JobStore) Get(id uuid.UUID) (*ConversionJob, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return job, ok
}

func (s *JobStore) CreateJob(userID uuid.UUID, jobID uuid.UUID, inputName, targetFormat string) (*ConversionJob, error) {
	targetFormat = strings.ToLower(strings.TrimPrefix(strings.TrimSpace(targetFormat), "."))
	if !isSupportedTarget(targetFormat) {
		return nil, eventtransport.NewError("unsupported_target_format", "Unsupported target format: "+targetFormat)
	}

	baseDir := filepath.Join(config.FilesRootPath, jobID.String())
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, err
	}

	inputExt := strings.ToLower(filepath.Ext(inputName))
	if inputExt == "" {
		inputExt = ".bin"
	}
	outputExt := outputExtension(targetFormat)
	job := &ConversionJob{
		ID:           jobID,
		UserID:       userID,
		InputName:    filepath.Base(inputName),
		TargetFormat: targetFormat,
		InputPath:    filepath.Join(baseDir, "input"+inputExt),
		OutputPath:   filepath.Join(baseDir, "output"+outputExt),
		Status:       JobStatusCreated,
		Done:         make(chan struct{}),
	}
	Jobs.Add(job)
	return job, nil
}

func (j *ConversionJob) Complete() {
	j.mu.Lock()
	j.Status = JobStatusDone
	j.mu.Unlock()

	j.emit(JobStatusDone, "")

	j.once.Do(func() {
		close(j.Done)
	})
}

func (j *ConversionJob) Fail(err string) {

	fmt.Printf("[Error] [Job] %s at %s", err, j.ID.String())
	j.mu.Lock()
	j.Status = JobStatusFailed
	j.Error = err
	j.mu.Unlock()

	j.emit(JobStatusFailed, err)

	j.once.Do(func() {
		close(j.Done)
	})
}

func (j *ConversionJob) SetStatus(status JobStatus) {
	j.mu.Lock()
	j.Status = status
	j.mu.Unlock()

	select {
	case j.Events <- JobEvent{Status: status}:
	default:
	}
}

func (j *ConversionJob) finish(status JobStatus, message string) {
	j.mu.Lock()
	j.Status = status
	j.Error = message
	j.mu.Unlock()
	j.once.Do(func() {
		close(j.Done)
	})
}

func (j *ConversionJob) snapshot() (JobStatus, string) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	return j.Status, j.Error
}

func (j *ConversionJob) emit(status JobStatus, err string) {
	select {
	case j.Events <- JobEvent{
		Status: status,
		Error:  err,
	}:
	default:
	}
}
