package wt

import (
	"FileConverter/eventtransport"
	"FileConverter/mediaconverter"
	"FileConverter/messageformat"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	webtransport "github.com/quic-go/webtransport-go"
)

const writeDeadline = 30 * time.Second

type heartbeatDatagram struct {
	Type string `json:"type" msgpack:"type" cbor:"type"`
	TS   int64  `json:"ts" msgpack:"ts" cbor:"ts"`
}

type Transport struct {
	session          *webtransport.Session
	controlStream    *webtransport.Stream
	streamManager    *StreamManager
	formatter        messageformat.Formatter
	maxMessageSize   int64
	heartbeatTimeout time.Duration
	writeMu          sync.Mutex
	heartbeatMu      sync.Mutex
	lastPongAt       time.Time
	closeOnce        sync.Once
	done             chan struct{}
}

type StreamHeader struct {
	Kind  StreamKind `json:"kind" msgpack:"kind" cbor:"kind"`
	JobID string     `json:"job_id" msgpack:"job_id" cbor:"job_id"`
}

func NewTransport(session *webtransport.Session, formatter messageformat.Formatter, maxMessageSize int64, heartbeatTimeout time.Duration) (*Transport, error) {
	ctx, cancel := context.WithTimeout(session.Context(), 10*time.Second)
	defer cancel()

	controlStream, err := session.AcceptStream(ctx)
	if err != nil {
		return nil, fmt.Errorf("accept control stream: %w", err)
	}

	return &Transport{
		session:          session,
		controlStream:    controlStream,
		streamManager:    NewStreamManager(),
		formatter:        formatter,
		maxMessageSize:   maxMessageSize,
		heartbeatTimeout: heartbeatTimeout,
		lastPongAt:       time.Now(),
		done:             make(chan struct{}),
	}, nil
}

func (t *Transport) Run(client *eventtransport.Client) {
	go t.acceptStreams(client)
	go t.readDatagrams()
	go t.watchHeartbeat()
}

func (t *Transport) Name() string {
	return "webtransport"
}

func (t *Transport) Marshal(v any) ([]byte, error) {
	return t.formatter.Marshal(v)
}

func (t *Transport) Unmarshal(data []byte, v any) error {
	return t.formatter.Unmarshal(data, v)
}

func (t *Transport) ReadMessage() ([]byte, error) {
	var frameLen uint32
	if err := binary.Read(t.controlStream, binary.BigEndian, &frameLen); err != nil {
		return nil, err
	}
	if int64(frameLen) > t.maxMessageSize {
		return nil, eventtransport.NewError("message_too_large", "Message is too large")
	}

	payload := make([]byte, int(frameLen))
	if _, err := io.ReadFull(t.controlStream, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (t *Transport) WriteMessage(message []byte) error {
	if int64(len(message)) > t.maxMessageSize {
		return eventtransport.NewError("message_too_large", "Message is too large")
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	if err := t.controlStream.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
		return err
	}

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(message)))
	if _, err := t.controlStream.Write(header[:]); err != nil {
		return err
	}
	_, err := t.controlStream.Write(message)
	return err
}

func (t *Transport) WritePing() error {
	payload, err := json.Marshal(heartbeatDatagram{
		Type: "ping",
		TS:   time.Now().UnixMilli(),
	})
	if err != nil {
		return err
	}
	return t.session.SendDatagram(payload)
}

func (t *Transport) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.done)
		_ = t.controlStream.Close()
		err = t.session.CloseWithError(0, "")
	})
	return err
}

func (t *Transport) readDatagrams() {
	for {
		payload, err := t.session.ReceiveDatagram(t.session.Context())
		if err != nil {
			return
		}

		var message heartbeatDatagram
		if err := json.Unmarshal(payload, &message); err != nil {
			continue
		}

		switch message.Type {
		case "ping":
			_ = t.sendPong(message.TS)
		case "pong":
			t.markPong()
		}
	}
}

func (t *Transport) watchHeartbeat() {
	interval := t.heartbeatTimeout / 4
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if time.Since(t.lastPong()) > t.heartbeatTimeout {
				_ = t.Close()
				return
			}
		case <-t.done:
			return
		}
	}
}

func (t *Transport) sendPong(ts int64) error {
	payload, err := json.Marshal(heartbeatDatagram{
		Type: "pong",
		TS:   ts,
	})
	if err != nil {
		return err
	}
	return t.session.SendDatagram(payload)
}

func (t *Transport) markPong() {
	t.heartbeatMu.Lock()
	t.lastPongAt = time.Now()
	t.heartbeatMu.Unlock()
}

func (t *Transport) lastPong() time.Time {
	t.heartbeatMu.Lock()
	defer t.heartbeatMu.Unlock()
	return t.lastPongAt
}

func (t *Transport) acceptStreams(client *eventtransport.Client) {
	for {
		stream, err := t.session.AcceptStream(t.session.Context())
		if err != nil {
			return
		}

		go t.handleStream(client, stream)
	}
}

func (t *Transport) handleStream(client *eventtransport.Client, stream *webtransport.Stream) {
	defer stream.Close()

	header, err := ReadHeader(stream)
	if err != nil {
		return
	}

	ds, err := NewDataStream(stream, header.Kind)
	if err != nil {
		return
	}
	if err := t.streamManager.StreamAppend(ds); err != nil {
		return
	}
	defer t.streamManager.Remove(ds.ID)

	switch header.Kind {
	case Upload:
		jobID, err := uuid.Parse(header.JobID)
		if err != nil {
			client.SendError("404", "JobID not found")
			stream.CancelRead(404)
			stream.CancelWrite(404)
			return
		}
		t.handleUpload(client, stream, jobID)
	case Download:
		jobID, err := uuid.Parse(header.JobID)
		if err != nil {
			client.SendError("404", "JobID not found")
			stream.CancelRead(404)
			stream.CancelWrite(404)
			return
		}
		t.handleDownload(client, stream, jobID)
	case PingPong:
		_, _ = io.Copy(stream, stream)
	}
}

func (t *Transport) handleUpload(client *eventtransport.Client, stream io.Reader, jobID uuid.UUID) {

	var job, ok = mediaconverter.Jobs.Get(jobID)
	if !ok {
		// TODO send to client that jobid not found
		return
	}

	job.SetStatus(mediaconverter.JobStatusUploading)

	client.SendAck(eventtransport.Progress{
		State: "Upload has been started",
	})

	file, err := os.Create(job.InputPath)
	if err != nil {
		client.SendError("500", "Input path creation failed")
		job.Fail(err.Error())
		return
	}
	if _, err = io.Copy(file, stream); err != nil {
		client.SendError("500", "File could not be created")
		_ = file.Close()
		job.Fail(err.Error())
		return
	}
	if err = file.Close(); err != nil {
		client.SendError("500", "File could not be closed")
		job.Fail(err.Error())
		return
	}

	job.SetStatus(mediaconverter.JobStatusUploaded)

	mediaconverter.ProcessingQueue <- job.ID

	client.SendAck(
		eventtransport.JobCreated{
			JobID: job.ID.String(),
		})
}

func (t *Transport) handleDownload(client *eventtransport.Client, stream io.Writer, jobID uuid.UUID) {
	job, ok := mediaconverter.Jobs.Get(jobID)
	if !ok {
		return
	}

	select {
	case <-job.Done:
	case <-t.done:
		return
	case <-time.After(30 * time.Minute):
		return
	}

	if job.Status != mediaconverter.JobStatusDone {
		fmt.Printf("[Error] [Download] Status not Done")
		return
	}

	file, err := os.Open(job.OutputPath)
	if err != nil {
		fmt.Printf("[Error] [Download] Output file missing")
		return
	}
	defer file.Close()
	_, _ = io.Copy(stream, file)
}

func WriteHeader(stream io.Writer, header StreamHeader) error {
	payload, err := json.Marshal(header)
	if err != nil {
		return err
	}

	if err := binary.Write(stream, binary.BigEndian, uint32(len(payload))); err != nil {
		return err
	}

	_, err = stream.Write(payload)
	return err
}

func ReadHeader(stream io.Reader) (*StreamHeader, error) {
	var size uint32
	if err := binary.Read(stream, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	if size > 4096 {
		return nil, fmt.Errorf("stream header is too large")
	}

	payload := make([]byte, size)
	if _, err := io.ReadFull(stream, payload); err != nil {
		return nil, err
	}

	var header StreamHeader
	if err := json.Unmarshal(payload, &header); err != nil {
		return nil, err
	}
	return &header, nil
}
