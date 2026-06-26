package eventtransport

import (
	"log"
	"sync"

	"github.com/google/uuid"
)

type Manager struct {
	clients       map[uuid.UUID]*Client
	userClients   map[uuid.UUID]map[uuid.UUID]*Client
	register      chan *Client
	unregister    chan *Client
	roomBroadcast chan *BroadcastMessage
	mu            sync.RWMutex
	workerPool    *WorkerPool
	done          chan bool
}

type EventTransportManager = Manager

type BroadcastMessage struct {
	RoomID  string
	Message []byte
	Sender  string
}

type WorkerPool struct {
	workers   int
	workQueue chan *BroadcastMessage
	quit      chan bool
	wg        sync.WaitGroup
}

func NewManager() *Manager {
	return &Manager{
		clients:       make(map[uuid.UUID]*Client),
		userClients:   make(map[uuid.UUID]map[uuid.UUID]*Client),
		register:      make(chan *Client, 256),
		unregister:    make(chan *Client, 256),
		roomBroadcast: make(chan *BroadcastMessage, 1024),
		workerPool:    NewWorkerPool(10), // 10 worker goroutines
		done:          make(chan bool),
	}
}

func NewWorkerPool(workers int) *WorkerPool {
	return &WorkerPool{
		workers:   workers,
		workQueue: make(chan *BroadcastMessage, 1024),
		quit:      make(chan bool),
	}
}

func (wp *WorkerPool) Start() {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
}

func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	for {
		select {
		case message := <-wp.workQueue:
			wp.processMessage(message)
		case <-wp.quit:
			return
		}
	}
}

func (wp *WorkerPool) processMessage(message *BroadcastMessage) {

	manager := GetManager()

	manager.mu.RLock()
	defer manager.mu.RUnlock()

	for clientID, client := range manager.clients {
		select {
		case client.Send <- message.Message:
		default:
			log.Printf("Client %s channel full, dropping message", clientID)
		}
	}
}

func (wp *WorkerPool) Stop() {
	close(wp.quit)
	wp.wg.Wait()
}

func (m *Manager) GetClient(userID uuid.UUID) (*Client, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, client := range m.userClients[userID] {
		return client, true
	}
	return nil, false
}

func (m *Manager) GetClients(userID uuid.UUID) []*Client {
	m.mu.RLock()
	defer m.mu.RUnlock()

	clients := make([]*Client, 0, len(m.userClients[userID]))
	for _, client := range m.userClients[userID] {
		clients = append(clients, client)
	}
	return clients
}

func (m *Manager) SendToUser(userID uuid.UUID, message []byte) {
	client, _ := m.GetClient(userID)
	_ = client.SendMessage(message)
}

func (m *Manager) Broadcast(roomID string, message []byte, senderID string) {
	roomMessage := &BroadcastMessage{
		RoomID:  roomID,
		Message: message,
		Sender:  senderID,
	}

	select {
	case m.workerPool.workQueue <- roomMessage:
	default:
		log.Printf("Worker queue full, dropping message for room %s", roomID)
	}
}

func (m *Manager) Run() {
	m.workerPool.Start()

	for {
		select {
		case client := <-m.register:
			m.mu.Lock()
			m.clients[client.ID] = client
			if m.userClients[client.ID] == nil {
				m.userClients[client.ID] = make(map[uuid.UUID]*Client)
			}
			m.userClients[client.ID][client.ID] = client
			m.mu.Unlock()
			log.Printf("Client registered: %s via %s", client.ID, client.Transport.Name())

		case client := <-m.unregister:
			m.mu.Lock()
			delete(m.clients, client.ID)
			if userClients := m.userClients[client.ID]; userClients != nil {
				delete(userClients, client.ID)
				if len(userClients) == 0 {
					delete(m.userClients, client.ID)
				}
			}
			m.mu.Unlock()

			client.Close()
			log.Printf("Client unregistered: %s via %s", client.ID, client.Transport.Name())

		case <-m.done:
			return
		}
	}
}

func (m *Manager) Shutdown() {
	close(m.done)
	m.workerPool.Stop()

	m.mu.Lock()
	for _, client := range m.clients {
		client.Close()
	}
	m.mu.Unlock()
}

func (m *Manager) RegisterClient(client *Client) {
	m.register <- client
}

func (m *Manager) UnregisterClient(client *Client) {
	m.unregister <- client
}

var (
	eventTransportManager *Manager
	once                  sync.Once
)

func GetManager() *Manager {
	once.Do(func() {
		eventTransportManager = NewManager()
		go eventTransportManager.Run()
	})
	return eventTransportManager
}
