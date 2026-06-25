// Relay Server for the room/relay architecture.
//
// Dependencies:
//   go mod init relay-server
//   go get github.com/gorilla/websocket
//
// Run:
//   go run . -config relay_config.json
//
// Config example:
// {
//   "relayName": "Local Relay",
//   "publicPort": "9000",
//   "registryUrl": "http://localhost:8080",
//   "region": "other",
//   "maxRooms": 1000,
//   "maxUsers": 10000,
//   "roomMaxUsers": 4,
//   "allowNewRooms": true,
//   "heartbeatSeconds": 30,
//   "roomTTLSeconds": 45,
//   "messageTTLSeconds": 300
// }
//
// This relay is RAM-only:
// - no database
// - no accounts
// - no permanent storage
// - rooms live in memory
// - messages live only until delivered/acked/expired

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultRoomMaxUsers = 4
	defaultMaxRooms     = 1000
	defaultMaxUsers     = 10000
	defaultHeartbeat    = 30 * time.Second
	defaultRoomTTL      = 45 * time.Second
	defaultMessageTTL   = 5 * time.Minute
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Config struct {
	RelayName  			 string `json:"relayName"`
	PublicPort 			 int    `json:"publicPort"`
	PublicURL        string `json:"publicUrl"`
	UseCloudflare    bool   `json:"useCloudflare"`
	RegistryURL      string `json:"registryUrl"`
	Region           string `json:"region"`
	MaxRooms         int    `json:"maxRooms"`
	MaxUsers         int    `json:"maxUsers"`
	RoomMaxUsers     int    `json:"roomMaxUsers"`
	AllowNewRooms    bool   `json:"allowNewRooms"`
	HeartbeatSeconds int    `json:"heartbeatSeconds"`
	RoomTTLSeconds   int    `json:"roomTTLSeconds"`
	MessageTTLSeconds int   `json:"messageTTLSeconds"`
}

type RelayServer struct {
	mu sync.RWMutex

	relayID   string
	relayName string
	publicPort int
	publicURL   string
	region    string
	useCloudflare bool
	registryURL string
	httpClient  *http.Client
	startedAt   time.Time

	heartbeatEvery time.Duration
	roomTTL        time.Duration
	messageTTL     time.Duration
	allowNewRooms  bool
	maxRooms       int
	maxUsers       int
	roomMaxUsers   int

	rooms map[string]*Room
}

type Room struct {
	mu             sync.RWMutex
	RoomID         string
	PinHash        string
	MaxUsers       int
	CreatedAt      int64
	UpdatedAt      int64
	LastActivityAt int64
	Clients        map[string]*Client
	Members        map[string]*Member
	Messages       map[string]*Message
	Postboxes      map[string][]*Message
}

type Member struct {
	UserID     string          `json:"userId"`
	Nickname   string          `json:"nickname"`
	Transport  string          `json:"transport"`
	PeerInfo   json.RawMessage `json:"peerInfo,omitempty"`
	Connected  bool            `json:"connected"`
	JoinedAt   int64           `json:"joinedAt"`
	LastSeenAt int64           `json:"lastSeenAt"`
}

type Client struct {
	srv        *RelayServer
	room       *Room
	conn       *websocket.Conn
	send       chan []byte
	once       sync.Once
	UserID     string
	Nickname   string
	Transport  string
	PeerInfo   json.RawMessage
	JoinedAt   int64
	LastSeenAt int64
}

type Message struct {
	MessageID   string          `json:"messageId"`
	SenderID    string          `json:"senderId"`
	Text        string          `json:"text"`
	CreatedAt   int64           `json:"createdAt"`
	ExpiresAt   int64           `json:"expiresAt"`
	DeliveredTo map[string]bool `json:"deliveredTo"`
	PendingAcks map[string]bool `json:"pendingAcks"`
}

type roomRegisterRequest struct {
	RoomID   string `json:"roomId"`
	PinHash  string `json:"pinHash,omitempty"`
	MaxUsers int    `json:"maxUsers,omitempty"`
}

type joinRequest struct {
	Type      string          `json:"type"`
	RoomID    string          `json:"roomId"`
	UserID    string          `json:"userId"`
	Nickname  string          `json:"nickname,omitempty"`
	Pin       string          `json:"pin,omitempty"`
	Transport string          `json:"transport,omitempty"`
	PeerInfo  json.RawMessage `json:"peerInfo,omitempty"`
}

type messageRequest struct {
	Type      string `json:"type"`
	MessageID string `json:"messageId,omitempty"`
	Text      string `json:"text,omitempty"`
}

type ackRequest struct {
	Type      string `json:"type"`
	MessageID string `json:"messageId"`
}

type signalRequest struct {
	Type         string          `json:"type"`
	TargetUserID string          `json:"targetUserId,omitempty"`
	SignalType   string          `json:"signalType,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
}

type roomSnapshot struct {
	RoomID         string   `json:"roomId"`
	MaxUsers       int      `json:"maxUsers"`
	ConnectedUsers int      `json:"connectedUsers"`
	MemberCount    int      `json:"memberCount"`
	MessageCount   int      `json:"messageCount"`
	CreatedAt      int64    `json:"createdAt"`
	UpdatedAt      int64    `json:"updatedAt"`
	LastActivityAt  int64    `json:"lastActivityAt"`
	Users          []Member `json:"users"`
}

type wsEnvelope struct {
	Type         string          `json:"type"`
	Ok           bool            `json:"ok,omitempty"`
	Error        string          `json:"error,omitempty"`
	RoomID       string          `json:"roomId,omitempty"`
	UserID       string          `json:"userId,omitempty"`
	Nickname     string          `json:"nickname,omitempty"`
	Transport    string          `json:"transport,omitempty"`
	PeerInfo     json.RawMessage `json:"peerInfo,omitempty"`
	MessageID    string          `json:"messageId,omitempty"`
	SenderID     string          `json:"senderId,omitempty"`
	Text         string          `json:"text,omitempty"`
	TargetUserID string          `json:"targetUserId,omitempty"`
	SignalType   string          `json:"signalType,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
	Room         *roomSnapshot   `json:"room,omitempty"`
	Users        []Member        `json:"users,omitempty"`
}

type registryRegisterResponse struct {
	RelayID string `json:"relayId"`
}

func main() {
	configPath := flag.String("config", "relay_config.json", "path to relay config json")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	srv := &RelayServer{
		relayID:        "",
		relayName:      cfg.RelayName,
		publicPort: 		cfg.PublicPort,
		region:         cfg.Region,
		publicURL:      cfg.PublicURL,
		useCloudflare:  cfg.UseCloudflare,
		registryURL:    strings.TrimRight(strings.TrimSpace(cfg.RegistryURL), "/"),
		httpClient:     &http.Client{Timeout: 5 * time.Second},
		startedAt:      time.Now().UTC(),
		heartbeatEvery: time.Duration(cfg.HeartbeatSeconds) * time.Second,
		roomTTL:        time.Duration(cfg.RoomTTLSeconds) * time.Second,
		messageTTL:     time.Duration(cfg.MessageTTLSeconds) * time.Second,
		allowNewRooms:  cfg.AllowNewRooms,
		maxRooms:       cfg.MaxRooms,
		maxUsers:       cfg.MaxUsers,
		roomMaxUsers:   cfg.RoomMaxUsers,
		rooms:          make(map[string]*Room),
	}

	if strings.TrimSpace(srv.relayName) == "" {
		srv.relayName = "Relay"
	}
	if srv.publicPort <= 0 {
    srv.publicPort = 80
	}
	if strings.TrimSpace(srv.region) == "" {
		srv.region = "other"
	}
	if srv.maxRooms <= 0 {
		srv.maxRooms = defaultMaxRooms
	}
	if srv.maxUsers <= 0 {
		srv.maxUsers = defaultMaxUsers
	}
	if srv.roomMaxUsers <= 0 {
		srv.roomMaxUsers = defaultRoomMaxUsers
	}
	if srv.heartbeatEvery <= 0 {
		srv.heartbeatEvery = defaultHeartbeat
	}
	if srv.roomTTL <= 0 {
		srv.roomTTL = defaultRoomTTL
	}
	if srv.messageTTL <= 0 {
		srv.messageTTL = defaultMessageTTL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if srv.registryURL != "" {
		if err := srv.registerWithRegistry(ctx); err != nil {
			log.Printf("registry registration failed: %v", err)
		}
		go srv.heartbeatLoop()
	}
	go srv.cleanupLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", srv.handleHealth)
	mux.HandleFunc("/ws", srv.handleWS)
	mux.HandleFunc("/api/rooms", srv.handleRooms)
	mux.HandleFunc("/api/rooms/", srv.handleRoomByID)
	mux.HandleFunc("/internal/rooms/register", srv.handleInternalRoomRegister)
	mux.HandleFunc("/internal/rooms/delete", srv.handleInternalRoomDelete)

	addr := fmt.Sprintf(":%d", srv.publicPort)
	server := &http.Server{
		Addr:              addr,
		Handler:           withMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
		// WebSocket connections are long-lived; per-request timeouts break upgrades.
		ReadTimeout:  0,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}

	log.Printf("relay server listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("relay server error: %v", err)
	}
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	cfg.RelayName = strings.TrimSpace(cfg.RelayName)
	cfg.RegistryURL = strings.TrimSpace(cfg.RegistryURL)
	cfg.Region = strings.TrimSpace(cfg.Region)

	if cfg.Region == "" {
		cfg.Region = "other"
	}
	if cfg.MaxRooms <= 0 {
		cfg.MaxRooms = defaultMaxRooms
	}
	if cfg.MaxUsers <= 0 {
		cfg.MaxUsers = defaultMaxUsers
	}
	if cfg.RoomMaxUsers <= 0 {
		cfg.RoomMaxUsers = defaultRoomMaxUsers
	}
	if cfg.HeartbeatSeconds <= 0 {
		cfg.HeartbeatSeconds = int(defaultHeartbeat / time.Second)
	}
	if cfg.RoomTTLSeconds <= 0 {
		cfg.RoomTTLSeconds = int(defaultRoomTTL / time.Second)
	}
	if cfg.MessageTTLSeconds <= 0 {
		cfg.MessageTTLSeconds = int(defaultMessageTTL / time.Second)
	}
	if cfg.PublicPort <= 0 {
    cfg.PublicPort = 9000
	}
	return &cfg, nil
}

func withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !isWebSocketRequest(r) {
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
		}
		next.ServeHTTP(w, r)
	})
}

func isWebSocketRequest(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

func (s *RelayServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	rooms, users := s.currentCounts()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true,
		"relayId": s.relayID,
		"relayName": s.relayName,
		"publicPort": s.publicPort,
		"publicURL": s.publicURL,
		"useCloudflare": s.useCloudflare,
		"region": s.region,
		"rooms": rooms,
		"users": users,
		"startedAt": s.startedAt.Unix(),
	})
}

func (s *RelayServer) handleRooms(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rooms": s.snapshotRooms()})
}

func (s *RelayServer) handleRoomByID(w http.ResponseWriter, r *http.Request) {
	roomID := strings.TrimPrefix(r.URL.Path, "/api/rooms/")
	roomID = strings.TrimSpace(roomID)
	if roomID == "" || strings.Contains(roomID, "/") {
		writeJSONError(w, http.StatusBadRequest, "room_id_required")
		return
	}

	switch r.Method {
	case http.MethodGet:
		room, ok := s.getRoom(roomID)
		if !ok {
			writeJSONError(w, http.StatusNotFound, "room_not_found")
			return
		}
		writeJSON(w, http.StatusOK, room.snapshot())
	case http.MethodDelete:
		s.deleteRoom(roomID)
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "roomId": roomID})
	default:
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
	}
}

func (s *RelayServer) handleInternalRoomRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req roomRegisterRequest
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	if strings.TrimSpace(req.RoomID) == "" {
		writeJSONError(w, http.StatusBadRequest, "room_id_required")
		return
	}
room := s.ensureRoom(req.RoomID, req.PinHash, req.MaxUsers)

if room == nil {
    writeJSONError(
        w,
        http.StatusServiceUnavailable,
        "room_creation_disabled",
    )
    return
}

writeJSON(w, http.StatusOK, room.snapshot())
}

func (s *RelayServer) handleInternalRoomDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req struct{ RoomID string `json:"roomId"` }
	if err := decodeJSON(r.Body, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_json")
		return
	}
	if strings.TrimSpace(req.RoomID) == "" {
		writeJSONError(w, http.StatusBadRequest, "room_id_required")
		return
	}
	s.deleteRoom(req.RoomID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "roomId": req.RoomID})
}

func (s *RelayServer) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("websocket upgrade failed: %v", err)
		return
	}
	client := &Client{
		srv:        s,
		conn:       conn,
		send:       make(chan []byte, 64),
		JoinedAt:   time.Now().UTC().Unix(),
		LastSeenAt: time.Now().UTC().Unix(),
	}
	go client.writePump()
	client.readPump()
}

func (c *Client) readPump() {
	defer c.cleanup()

	c.conn.SetReadLimit(1 << 20)
	_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		var raw map[string]any
		if err := c.conn.ReadJSON(&raw); err != nil {
			return
		}
		c.LastSeenAt = time.Now().UTC().Unix()
		typeName, _ := raw["type"].(string)
		switch typeName {
		case "join_room":
			var req joinRequest
			if err := mapToStruct(raw, &req); err != nil {
				c.writeError("invalid_join_request")
				continue
			}
			c.handleJoin(req)
		case "leave_room":
			c.handleLeave()
		case "message":
			var req messageRequest
			if err := mapToStruct(raw, &req); err != nil {
				c.writeError("invalid_message_request")
				continue
			}
			c.handleMessage(req)
		case "ack":
			var req ackRequest
			if err := mapToStruct(raw, &req); err != nil {
				c.writeError("invalid_ack_request")
				continue
			}
			c.handleAck(req)
		case "signal":
			var req signalRequest
			if err := mapToStruct(raw, &req); err != nil {
				c.writeError("invalid_signal_request")
				continue
			}
			c.handleSignal(req)
		case "ping":
			c.sendJSON(wsEnvelope{Type: "pong", Ok: true})
		default:
			c.writeError("unknown_type")
		}
	}
}

func (c *Client) writePump() {
	pingTicker := time.NewTicker(30 * time.Second)
	defer func() {
		pingTicker.Stop()
		_ = c.conn.Close()
	}()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-pingTicker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, []byte("ping")); err != nil {
				return
			}
		}
	}
}

func (c *Client) handleJoin(req joinRequest) {
	if strings.TrimSpace(req.RoomID) == "" || strings.TrimSpace(req.UserID) == "" {
		c.writeError("room_id_and_user_id_required")
		return
	}
	if strings.TrimSpace(req.Nickname) == "" {
		req.Nickname = req.UserID
	}
	if strings.TrimSpace(req.Transport) == "" {
		req.Transport = "relay"
	}


	room, ok := c.srv.getRoom(req.RoomID)
if !ok {
    c.writeError("room_not_found")
    return
}

	room.mu.Lock()

	if room.PinHash != "" {
		if req.Pin == "" {
			room.mu.Unlock()
			c.writeError("pin_required")
			return
		}
		if hashPin(req.Pin) != room.PinHash {
			room.mu.Unlock()
			c.writeError("pin_invalid")
			return
		}
	}

	if len(room.Clients) >= room.MaxUsers {
		if existing, ok := room.Clients[req.UserID]; !ok || existing == nil {
			room.mu.Unlock()
			c.writeError("room_full")
			return
		}
	}

	if existing, ok := room.Clients[req.UserID]; ok && existing != nil && existing != c {
		existing.forceClose()
	}

	now := time.Now().UTC().Unix()
	member, ok := room.Members[req.UserID]
	if !ok {
		member = &Member{UserID: req.UserID, JoinedAt: now}
		room.Members[req.UserID] = member
	}
	member.Nickname = req.Nickname
	member.Transport = req.Transport
	member.PeerInfo = append(json.RawMessage(nil), req.PeerInfo...)
	member.Connected = true
	member.LastSeenAt = now

	room.Clients[req.UserID] = c
	room.LastActivityAt = now
	room.UpdatedAt = now

	c.room = room
	c.UserID = req.UserID
	c.Nickname = req.Nickname
	c.Transport = req.Transport
	c.PeerInfo = append(json.RawMessage(nil), req.PeerInfo...)
	c.JoinedAt = member.JoinedAt
	c.LastSeenAt = now
	room.mu.Unlock()
	snap := room.snapshot()
	c.sendJSON(wsEnvelope{
		Type:   "room_joined",
		Ok:     true,
		Room:   &snap,
		Users:  room.connectedMembersSnapshot(),
		UserID: req.UserID,
		RoomID: req.RoomID,
	})

	room.broadcastExcept(req.UserID, wsEnvelope{
		Type:      "user_joined",
		RoomID:    room.RoomID,
		UserID:    req.UserID,
		Nickname:  req.Nickname,
		Transport: req.Transport,
		PeerInfo:  req.PeerInfo,
	})

	if queued := room.Postboxes[req.UserID]; len(queued) > 0 {
		for _, m := range queued {
			c.sendJSON(wsEnvelope{
				Type:      "message",
				RoomID:    room.RoomID,
				MessageID: m.MessageID,
				SenderID:  m.SenderID,
				Text:      m.Text,
			})
		}
	}

	
}

func (c *Client) handleLeave() {
	if c.room == nil || c.UserID == "" {
		return
	}
	room := c.room
	room.removeClient(c.UserID)
	room.broadcastExcept(c.UserID, wsEnvelope{Type: "user_left", RoomID: room.RoomID, UserID: c.UserID})
	c.room = nil
	c.UserID = ""
}

func (c *Client) handleMessage(req messageRequest) {
	if c.room == nil || c.UserID == "" {
		c.writeError("join_room_first")
		return
	}
	text := strings.TrimSpace(req.Text)
	if text == "" {
		c.writeError("empty_message")
		return
	}
	room := c.room
	now := time.Now().UTC().Unix()
	msgID := strings.TrimSpace(req.MessageID)
	if msgID == "" {
		msgID = newID("msg")
	}

	room.mu.Lock()

	msg := &Message{
		MessageID:   msgID,
		SenderID:    c.UserID,
		Text:        text,
		CreatedAt:   now,
		ExpiresAt:   now + int64(c.srv.messageTTL.Seconds()),
		DeliveredTo: make(map[string]bool),
		PendingAcks: make(map[string]bool),
	}

	for userID := range room.Members {
		if userID == c.UserID {
			continue
		}
		msg.PendingAcks[userID] = true
		if client, ok := room.Clients[userID]; ok && client != nil {
			msg.DeliveredTo[userID] = true
			client.sendJSON(wsEnvelope{
				Type:      "message",
				RoomID:    room.RoomID,
				MessageID: msg.MessageID,
				SenderID:  c.UserID,
				Text:      text,
			})
		} else {
			room.Postboxes[userID] = append(room.Postboxes[userID], msg)
		}
	}

	room.Messages[msgID] = msg
	room.LastActivityAt = now
	room.UpdatedAt = now

	c.sendJSON(wsEnvelope{
		Type:      "message_accepted",
		Ok:        true,
		RoomID:    room.RoomID,
		MessageID: msgID,
	})

	if len(msg.PendingAcks) == 0 {
		delete(room.Messages, msgID)
	}
	room.mu.Unlock()
}

func (c *Client) handleAck(req ackRequest) {
	if c.room == nil || c.UserID == "" {
		return
	}
	room := c.room
	room.mu.Lock()
	defer room.mu.Unlock()

	msg, ok := room.Messages[strings.TrimSpace(req.MessageID)]
	if !ok {
		return
	}
	delete(msg.PendingAcks, c.UserID)
	msg.DeliveredTo[c.UserID] = true
	room.removeFromPostboxLocked(c.UserID, msg.MessageID)
	if len(msg.PendingAcks) == 0 {
		delete(room.Messages, msg.MessageID)
		room.removeFromAllPostboxesLocked(msg.MessageID)
	}
}

func (c *Client) handleSignal(req signalRequest) {
	if c.room == nil || c.UserID == "" {
		c.writeError("join_room_first")
		return
	}
	if strings.TrimSpace(req.TargetUserID) == "" {
		c.writeError("target_user_id_required")
		return
	}
	room := c.room
	room.mu.RLock()
	target, ok := room.Clients[req.TargetUserID]
	room.mu.RUnlock()
	if !ok || target == nil {
		c.writeError("target_offline")
		return
	}
	target.sendJSON(wsEnvelope{
		Type:         "signal",
		RoomID:       room.RoomID,
		UserID:       c.UserID,
		Nickname:     c.Nickname,
		TargetUserID: req.TargetUserID,
		SignalType:   req.SignalType,
		Data:         req.Data,
	})
}

func (c *Client) sendJSON(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		return
	}
	select {
	case c.send <- data:
	default:
	}
}

func (c *Client) writeError(msg string) {
	c.sendJSON(wsEnvelope{Type: "error", Error: msg})
}

func (c *Client) cleanup() {
	c.once.Do(func() {
		if c.room != nil && c.UserID != "" {
			room := c.room
			room.removeClient(c.UserID)
			room.broadcastExcept(c.UserID, wsEnvelope{Type: "user_left", RoomID: room.RoomID, UserID: c.UserID})
			if room.shouldDelete(c.srv.roomTTL) {
				c.srv.deleteRoom(room.RoomID)
			}
			c.room = nil
		}
		close(c.send)
		_ = c.conn.Close()
	})
}

func (c *Client) forceClose() {
	_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	_ = c.conn.Close()
}

func (s *RelayServer) registerWithRegistry(ctx context.Context) error {
	publicURL := s.publicURL
	if s.useCloudflare {
		publicURL = "https://" + publicURL
	}
	payload := map[string]any{
		"relayId":   s.relayID,
		"relayName": s.relayName,
		"publicPort": s.publicPort,
		"publicURL": s.publicURL,
		"region":    s.region,
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.registryURL+"/api/relay/register", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("registry register failed: %s", strings.TrimSpace(string(b)))
	}
	var out registryRegisterResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return err
	}
	if strings.TrimSpace(out.RelayID) != "" {
		s.relayID = out.RelayID
	}
	return nil
}

func (s *RelayServer) heartbeatLoop() {
	ticker := time.NewTicker(s.heartbeatEvery)
	defer ticker.Stop()
	for range ticker.C {
		if s.registryURL == "" {
			continue
		}
		if strings.TrimSpace(s.relayID) == "" {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = s.registerWithRegistry(ctx)
			cancel()
		}
		rooms, users := s.currentCounts()
		payload := map[string]any{
			"relayId":      s.relayID,
			"currentRooms": rooms,
			"currentUsers": users,
			"region":       s.region,
		}
		body, _ := json.Marshal(payload)
		req, err := http.NewRequest(http.MethodPost, s.registryURL+"/api/relay/heartbeat", bytes.NewReader(body))
		if err != nil {
			log.Printf("heartbeat request error: %v", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.httpClient.Do(req)
		if err != nil {
			log.Printf("heartbeat error: %v", err)
			continue
		}

		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}
}

func (s *RelayServer) cleanupLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now().UTC().Unix()
		s.mu.Lock()
		for roomID, room := range s.rooms {
			room.mu.Lock()
			for msgID, msg := range room.Messages {
				if now > msg.ExpiresAt {
					delete(room.Messages, msgID)
					room.removeFromAllPostboxesLocked(msgID)
				}
			}
			stale := len(room.Clients) == 0 && len(room.Messages) == 0 && len(room.Postboxes) == 0 && now-room.LastActivityAt > int64(s.roomTTL.Seconds())
			room.mu.Unlock()
			if stale {
				delete(s.rooms, roomID)
			}
		}
		s.mu.Unlock()
	}
}

func (s *RelayServer) currentCounts() (rooms int, users int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rooms = len(s.rooms)
	for _, room := range s.rooms {
		room.mu.RLock()
		users += len(room.Clients)
		room.mu.RUnlock()
	}
	return
}

func (s *RelayServer) snapshotRooms() []roomSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]roomSnapshot, 0, len(s.rooms))
	for _, room := range s.rooms {
		out = append(out, room.snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RoomID < out[j].RoomID })
	return out
}

func (s *RelayServer) getRoom(roomID string) (*Room, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	room, ok := s.rooms[roomID]
	return room, ok
}

func (s *RelayServer) ensureRoom(roomID, pinHash string, maxUsers int) *Room {
	s.mu.Lock()
	defer s.mu.Unlock()
	if room, ok := s.rooms[roomID]; ok {
		room.mu.Lock()
		if pinHash != "" {
			room.PinHash = pinHash
		}
		if maxUsers > 0 {
			if maxUsers > s.roomMaxUsers {
				maxUsers = s.roomMaxUsers
			}
			room.MaxUsers = maxUsers
		}
		room.UpdatedAt = time.Now().UTC().Unix()
		room.mu.Unlock()
		return room
	}
	if !s.allowNewRooms {
		return nil
	}
	if len(s.rooms) >= s.maxRooms {
		return nil
	}
	if maxUsers <= 0 || maxUsers > s.roomMaxUsers {
		maxUsers = s.roomMaxUsers
	}
	now := time.Now().UTC().Unix()
	room := &Room{
		RoomID:         roomID,
		PinHash:        pinHash,
		MaxUsers:       maxUsers,
		CreatedAt:      now,
		UpdatedAt:      now,
		LastActivityAt: now,
		Clients:        make(map[string]*Client),
		Members:        make(map[string]*Member),
		Messages:       make(map[string]*Message),
		Postboxes:      make(map[string][]*Message),
	}
	s.rooms[roomID] = room
	return room
}

func (s *RelayServer) deleteRoom(roomID string) {
	s.mu.Lock()
	room, ok := s.rooms[roomID]
	if ok {
		delete(s.rooms, roomID)
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	room.mu.Lock()
	clients := make([]*Client, 0, len(room.Clients))
	for _, c := range room.Clients {
		if c != nil {
			clients = append(clients, c)
		}
	}
	room.mu.Unlock()
	for _, c := range clients {
		c.forceClose()
	}
}

func (room *Room) snapshot() roomSnapshot {
	room.mu.RLock()
	defer room.mu.RUnlock()
	users := make([]Member, 0, len(room.Members))
	connected := 0
	for _, m := range room.Members {
		users = append(users, *m)
		if m.Connected {
			connected++
		}
	}
	return roomSnapshot{
		RoomID:         room.RoomID,
		MaxUsers:       room.MaxUsers,
		ConnectedUsers: connected,
		MemberCount:    len(room.Members),
		MessageCount:   len(room.Messages),
		CreatedAt:      room.CreatedAt,
		UpdatedAt:      room.UpdatedAt,
		LastActivityAt: room.LastActivityAt,
		Users:          users,
	}
}

func (room *Room) connectedMembersSnapshot() []Member {
	room.mu.RLock()
	defer room.mu.RUnlock()
	out := make([]Member, 0, len(room.Members))
	for _, m := range room.Members {
		if m.Connected {
			out = append(out, *m)
		}
	}
	return out
}

func (room *Room) removeClient(userID string) {
	room.mu.Lock()
	defer room.mu.Unlock()
	if member, ok := room.Members[userID]; ok {
		member.Connected = false
		member.LastSeenAt = time.Now().UTC().Unix()
	}
	delete(room.Clients, userID)
	room.LastActivityAt = time.Now().UTC().Unix()
	room.UpdatedAt = time.Now().UTC().Unix()
}

func (room *Room) shouldDelete(roomTTL time.Duration) bool {
	room.mu.RLock()
	defer room.mu.RUnlock()
	return len(room.Clients) == 0 && len(room.Messages) == 0 && len(room.Postboxes) == 0 && time.Now().UTC().Unix()-room.LastActivityAt > int64(roomTTL.Seconds())
}

func (room *Room) broadcastExcept(exceptUserID string, evt wsEnvelope) {
	room.mu.RLock()
	targets := make([]*Client, 0, len(room.Clients))
	for userID, client := range room.Clients {
		if userID == exceptUserID || client == nil {
			continue
		}
		targets = append(targets, client)
	}
	room.mu.RUnlock()
	for _, client := range targets {
		client.sendJSON(evt)
	}
}

func (room *Room) removeFromPostboxLocked(userID, messageID string) {
	queue := room.Postboxes[userID]
	if len(queue) == 0 {
		return
	}
	filtered := queue[:0]
	for _, msg := range queue {
		if msg == nil || msg.MessageID == messageID {
			continue
		}
		filtered = append(filtered, msg)
	}
	if len(filtered) == 0 {
		delete(room.Postboxes, userID)
		return
	}
	room.Postboxes[userID] = filtered
}

func (room *Room) removeFromAllPostboxesLocked(messageID string) {
	for userID := range room.Postboxes {
		room.removeFromPostboxLocked(userID, messageID)
	}
}

func hashPin(pin string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(pin)))
	return hex.EncodeToString(sum[:])
}

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	}
	return fmt.Sprintf("%s-%s", prefix, hex.EncodeToString(b[:]))
}

func mapToStruct(in map[string]any, out any) error {
	buf, err := json.Marshal(in)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(buf))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}

func decodeJSON(body io.ReadCloser, dst any) error {
	defer body.Close()
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, wsEnvelope{Type: "error", Error: msg})
}

