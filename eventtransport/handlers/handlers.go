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
	Type  string         `json:"type" msgpack:"type" cbor:"type"`
	Data  map[string]any `json:"data" msgpack:"data" cbor:"data"`
	Nonce string         `json:"nonce,omitempty" msgpack:"nonce,omitempty" cbor:"nonce,omitempty"`
}

type EncodeRequest struct {
	JobID        uuid.UUID `json:"job_id" msgpack:"job_id" cbor:"job_id"`
	InputName    string    `json:"input_name" msgpack:"input_name" cbor:"input_name"`
	TargetFormat string    `json:"target_format" msgpack:"target_format" cbor:"target_format"`
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

	switch msg.Type {
	case eventtransport.MessageTypeFileEncodeRequest:
		handleFileEncoding(client, msg.Data)
	default:
		client.SendError("unknown_type", fmt.Sprintf("Unknown message type: %s", msg.Type))
	}
}

func handleFileEncoding(client *eventtransport.Client, raw map[string]any) {
	_, ok := client.Transport.(*eventwt.Transport)
	if !ok {
		client.SendError("unsupported_transport", "File encoding requires WebTransport")
		return
	}

	JobID, err := uuid.NewV7()
	if err != nil {
		client.SendError("unable_to_generate_job_id", "Unable to generate JobID")
		return
	}

	req := EncodeRequest{
		JobID:        JobID,
		InputName:    stringValue(raw["input_name"]),
		TargetFormat: stringValue(raw["target_format"]),
	}
	if req.TargetFormat == "" {
		req.TargetFormat = stringValue(raw["format"])
	}
	if req.InputName == "" {
		req.InputName = "input.bin"
	}
	if req.TargetFormat == "" {
		client.SendError("invalid_request", "target_format is required")
		return
	}

	_, err = mediaconverter.Jobs.CreateJob(client.ID, req.JobID, req.InputName, req.TargetFormat)
	if err != nil {
		client.SendError("system_message", err.Error())
		return
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
