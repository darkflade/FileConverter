package eventhandlers

import (
	"FileConverter/eventtransport"
	eventwt "FileConverter/eventtransport/wt"
	"FileConverter/mediaconverter"
	"FileConverter/messageformat"
	"FileConverter/messageformat/formaters"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/quic-go/webtransport-go"
)

const (
	heartbeatWait  = 25 * time.Second
	pingPeriod     = 20 * time.Second
	writeWait      = 10 * time.Second
	maxMessageSize = 65536
)

type EventMessage struct {
	Nonce   string                             `json:"nonce,omitempty"`
	Payload eventtransport.EventMessagePayload `json:"payload"`
}

type EncodeRequest struct {
	InputName    string
	TargetFormat string
}

func (EncodeRequest) MessageName() string {
	return "encode_request"
}

func HandleWebTransport(server *webtransport.Server) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		log.Println("Handle WebTransport Request")

		session, err := server.Upgrade(w, r)
		if err != nil {
			log.Printf("webtransport upgrade error: %v", err)
			return
		}

		format, err := messageformat.ParseFormat(r.URL.Query().Get("protocol"))
		if err != nil {
			log.Printf("webtransport protocol error: %v", err)
			_ = session.CloseWithError(1, err.Error())
			return
		}
		formatter, err := formaters.New(format)
		if err != nil {
			log.Printf("webtransport formatter error: %v", err)
			_ = session.CloseWithError(1, err.Error())
			return
		}

		transport, err := eventwt.NewTransport(session, formatter, maxMessageSize, heartbeatWait)
		if err != nil {
			log.Printf("webtransport stream setup error: %v", err)
			_ = session.CloseWithError(0, "stream setup failed")
			return
		}
		serveEventConnection(transport)
	}
}

func serveEventConnection(transport eventtransport.EventTransport) {
	manager := eventtransport.GetManager()
	client := eventtransport.NewClient(transport)

	if client == nil {
		fmt.Printf("[Error] [Client] uuid error")
		return
	}

	manager.RegisterClient(client)
	if wtTransport, ok := transport.(*eventwt.Transport); ok {
		wtTransport.Run(client)
	}

	go writePump(client)
	go readPump(client)
}

func HandleMessage(client *eventtransport.Client, msg EventMessage) {
	client.SetRequestNonce(msg.Nonce)
	defer client.ClearRequestNonce()

	if !client.CheckRateLimit() {
		client.SendError("rate_limit", "Too many messages sent")
		return
	}

	//TODO make handler structure
	//type Handler func(*Client, Message)
	//
	//var handlers = map[string]Handler{}

	//func RegisterHandler(name string, h Handler) {
	//	handlers[name] = h
	//}
	//func init() {
	//	RegisterHandler("encode_request", func(c *Client, m Message) {
	//		handleFileEncoding(c, m.(*EncodeRequest))
	//	})
	//}
	//handler, ok := handlers[msg.Payload.MessageName()]
	//if !ok {
	//	client.SendError(
	//		"unknown_message",
	//		msg.Payload.MessageName(),
	//	)
	//	return
	//}
	//
	//handler(client, msg.Payload)
	switch payload := msg.Payload.(type) {
	case *EncodeRequest:
		handleFileEncoding(client, payload)

	default:

		client.SendError(
			"unknown_type",
			fmt.Sprintf("Unknown message type: %T", payload),
		)
	}
}

func handleFileEncoding(client *eventtransport.Client, req *EncodeRequest) {
	_, ok := client.Transport.(*eventwt.Transport)
	if !ok {
		client.SendError("400", "File encoding requires WebTransport")
		return
	}

	jobID, err := uuid.NewV7()
	if err != nil {
		client.SendError("500", "Unable to generate JobID")
		return
	}

	if req.TargetFormat == "" {
		client.SendError("400", "Target format is required")
		return
	}
	if req.InputName == "" {
		client.SendError("400", "Input name is required")
		return
	}

	job, err := mediaconverter.Jobs.CreateJob(client.ID, jobID, req.InputName, req.TargetFormat)
	if err != nil {
		client.SendError("system_message", err.Error())
		return
	}

	go observeJob(client, job)
}

func observeJob(client *eventtransport.Client, job *mediaconverter.ConversionJob) {
	for event := range job.Events {

		switch event.Status {

		case mediaconverter.JobStatusDone:

			client.SendAck(eventtransport.JobFinished{
				JobID: job.ID,
			})

			return

		case mediaconverter.JobStatusFailed:

			client.SendAck(eventtransport.JobFailed{
				JobID:  job.ID,
				Reason: event.Error,
			})

			return
		}
	}
}

func stringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	default:
		return ""
	}
}

func writePump(client *eventtransport.Client) {
	manager := eventtransport.GetManager()
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		manager.UnregisterClient(client)
	}()

	for {
		select {
		case msg, ok := <-client.Send:
			if !ok {
				return
			}
			if err := client.Transport.WriteMessage(msg); err != nil {
				log.Printf("Write error for client %s: %v", client.ID, err)
				log.Printf("Messages: %s", msg)
				return
			}
		case <-ticker.C:
			if err := client.Transport.WritePing(); err != nil {
				log.Printf("Ping error for client %s: %v", client.ID, err)
				return
			}
		}
	}
}

func readPump(client *eventtransport.Client) {
	manager := eventtransport.GetManager()
	defer func() {
		manager.UnregisterClient(client)
	}()

	for {
		data, err := client.Transport.ReadMessage()
		if err != nil {
			if client.Transport.Name() == "websocket" && websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error for client %s: %v", client.ID, err)
			} else if client.Transport.Name() == "webtransport" {
				log.Printf("WebTransport read closed for client %s: %v", client.ID, err)
			}
			break
		}

		var msg EventMessage
		if err := client.Transport.Unmarshal(data, &msg); err != nil {
			client.SendError("invalid_message", "Invalid message format")
			continue
		}

		HandleMessage(client, msg)
	}
}
