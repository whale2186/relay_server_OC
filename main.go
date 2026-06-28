// Relay Server for the room/relay architecture.
//
// Dependencies:
//   go mod init relay-server
//   go get github.com/gorilla/websocket
//   go get go.mongodb.org/mongo-driver/v2
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
//   "messageTTLSeconds": 300,
//   "offlineMessagesEnabled": false,
//   "mongoUri": "",
//   "mongoDatabase": "openchat_relay",
//   "mongoMessagesCollection": "messages",
//   "offlineMessageRetentionSeconds": 604800,
//   "messageSyncLimit": 200
// }
//
// This relay keeps online delivery in memory. Rooms can opt into MongoDB-backed
// offline message sync with offlineMessagesEnabled.

package main

import (
	"bufio"
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
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	defaultRoomMaxUsers    = 4
	defaultMaxRooms        = 1000
	defaultMaxUsers        = 10000
	defaultHeartbeat       = 30 * time.Second
	defaultRoomTTL         = 45 * time.Second
	defaultMessageTTL      = 5 * time.Minute
	defaultOfflineTTL      = 7 * 24 * time.Hour
	defaultSyncLimit       = 200
	defaultMongoDatabase   = "openchat_relay"
	defaultMongoCollection = "messages"

	messageStatusPending   = "pending"
	messageStatusSent      = "sent"
	messageStatusDelivered = "delivered"
	messageStatusRead      = "read"
)

var cfRegex = regexp.MustCompile(`https://[a-z0-9-]+\.trycloudflare\.com`)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type Config struct {
	RelayName                      string `json:"relayName"`
	PublicPort                     int    `json:"publicPort"`
	PublicURL                      string `json:"publicUrl"`
	UseCloudflare                  bool   `json:"useCloudflare"`
	RegistryURL                    string `json:"registryUrl"`
	Region                         string `json:"region"`
	MaxRooms                       int    `json:"maxRooms"`
	MaxUsers                       int    `json:"maxUsers"`
	RoomMaxUsers                   int    `json:"roomMaxUsers"`
	AllowNewRooms                  bool   `json:"allowNewRooms"`
	HeartbeatSeconds               int    `json:"heartbeatSeconds"`
	RoomTTLSeconds                 int    `json:"roomTTLSeconds"`
	MessageTTLSeconds              int    `json:"messageTTLSeconds"`
	OfflineMessagesEnabled         bool   `json:"offlineMessagesEnabled"`
	MongoURI                       string `json:"mongoUri"`
	MongoDatabase                  string `json:"mongoDatabase"`
	MongoMessagesCollection        string `json:"mongoMessagesCollection"`
	OfflineMessageRetentionSeconds int    `json:"offlineMessageRetentionSeconds"`
	MessageSyncLimit               int    `json:"messageSyncLimit"`
}

type RelayServer struct {
	mu sync.RWMutex

	relayID       string
	relayName     string
	publicPort    int
	publicURL     string
	region        string
	useCloudflare bool
	registryURL   string
	httpClient    *http.Client
	startedAt     time.Time

	heartbeatEvery   time.Duration
	roomTTL          time.Duration
	messageTTL       time.Duration
	offlineTTL       time.Duration
	allowNewRooms    bool
	maxRooms         int
	maxUsers         int
	roomMaxUsers     int
	offlineDefault   bool
	messageSyncLimit int
	messageStore     *mongoMessageStore

	rooms map[string]*Room

	cloudflaredCmd     *exec.Cmd
	cloudflareMu       sync.Mutex
	cloudflareDone     chan struct{}
	cloudflareURLReady chan struct{}
}

type Room struct {
	mu                     sync.RWMutex
	RoomID                 string
	PinHash                string
	MaxUsers               int
	CreatedAt              int64
	UpdatedAt              int64
	LastActivityAt         int64
	OfflineMessagesEnabled bool
	Clients                map[string]*Client
	Members                map[string]*Member
	Messages               map[string]*Message
	Postboxes              map[string][]*Message
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
	MessageID      string            `json:"messageId"`
	RoomID         string            `json:"roomId"`
	SenderID       string            `json:"senderId"`
	Text           string            `json:"text"`
	CreatedAt      int64             `json:"createdAt"`
	UpdatedAt      int64             `json:"updatedAt"`
	ExpiresAt      int64             `json:"expiresAt"`
	DeliveryStatus map[string]string `json:"deliveryStatus"`
	PendingAcks    map[string]bool   `json:"pendingAcks" bson:"-"`
}

type roomRegisterRequest struct {
	RoomID                 string `json:"roomId"`
	PinHash                string `json:"pinHash,omitempty"`
	MaxUsers               int    `json:"maxUsers,omitempty"`
	OfflineMessagesEnabled *bool  `json:"offlineMessagesEnabled,omitempty"`
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
	Status    string `json:"status,omitempty"`
}

type signalRequest struct {
	Type         string          `json:"type"`
	TargetUserID string          `json:"targetUserId,omitempty"`
	SignalType   string          `json:"signalType,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
}

type roomSnapshot struct {
	RoomID                 string   `json:"roomId"`
	MaxUsers               int      `json:"maxUsers"`
	ConnectedUsers         int      `json:"connectedUsers"`
	MemberCount            int      `json:"memberCount"`
	MessageCount           int      `json:"messageCount"`
	CreatedAt              int64    `json:"createdAt"`
	UpdatedAt              int64    `json:"updatedAt"`
	LastActivityAt         int64    `json:"lastActivityAt"`
	OfflineMessagesEnabled bool     `json:"offlineMessagesEnabled"`
	Users                  []Member `json:"users"`
}

type wsEnvelope struct {
	Type                   string            `json:"type"`
	Ok                     bool              `json:"ok,omitempty"`
	Error                  string            `json:"error,omitempty"`
	RoomID                 string            `json:"roomId,omitempty"`
	UserID                 string            `json:"userId,omitempty"`
	Nickname               string            `json:"nickname,omitempty"`
	Transport              string            `json:"transport,omitempty"`
	PeerInfo               json.RawMessage   `json:"peerInfo,omitempty"`
	MessageID              string            `json:"messageId,omitempty"`
	SenderID               string            `json:"senderId,omitempty"`
	Text                   string            `json:"text,omitempty"`
	TargetUserID           string            `json:"targetUserId,omitempty"`
	SignalType             string            `json:"signalType,omitempty"`
	Data                   json.RawMessage   `json:"data,omitempty"`
	CreatedAt              int64             `json:"createdAt,omitempty"`
	UpdatedAt              int64             `json:"updatedAt,omitempty"`
	Status                 string            `json:"status,omitempty"`
	DeliveryStatus         map[string]string `json:"deliveryStatus,omitempty"`
	OfflineMessagesEnabled bool              `json:"offlineMessagesEnabled,omitempty"`
	Room                   *roomSnapshot     `json:"room,omitempty"`
	Users                  []Member          `json:"users,omitempty"`
}

type messageHistoryResponse struct {
	RoomID   string                     `json:"roomId"`
	Messages []persistedMessageResponse `json:"messages"`
	HasMore  bool                       `json:"hasMore"`
}

type persistedMessageResponse struct {
	MessageID      string            `json:"messageId"`
	RoomID         string            `json:"roomId"`
	SenderID       string            `json:"senderId"`
	Text           string            `json:"text"`
	CreatedAt      int64             `json:"createdAt"`
	UpdatedAt      int64             `json:"updatedAt"`
	DeliveryStatus map[string]string `json:"deliveryStatus,omitempty"`
}

type registryRegisterResponse struct {
	RelayID string `json:"relayId"`
}

type storedMessage struct {
	MessageID      string                 `bson:"messageId"`
	RoomID         string                 `bson:"roomId"`
	SenderID       string                 `bson:"senderId"`
	Text           string                 `bson:"text"`
	CreatedAt      int64                  `bson:"createdAt"`
	UpdatedAt      int64                  `bson:"updatedAt"`
	ExpiresAt      int64                  `bson:"expiresAt"`
	DeliveryStatus []storedDeliveryStatus `bson:"deliveryStatus"`
}

type storedDeliveryStatus struct {
	UserID    string `bson:"userId"`
	Status    string `bson:"status"`
	UpdatedAt int64  `bson:"updatedAt"`
}

type mongoMessageStore struct {
	client     *mongo.Client
	collection *mongo.Collection
}

func newMongoMessageStore(ctx context.Context, cfg *Config) (*mongoMessageStore, error) {
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.MongoURI).SetConnectTimeout(10 * time.Second))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	store := &mongoMessageStore{
		client:     client,
		collection: client.Database(cfg.MongoDatabase).Collection(cfg.MongoMessagesCollection),
	}
	if err := store.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	return store, nil
}

func (s *mongoMessageStore) ensureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys: bson.D{{"roomId", 1}, {"messageId", 1}},
			Options: options.Index().
				SetName("room_message_unique").
				SetUnique(true),
		},
		{
			Keys: bson.D{{"roomId", 1}, {"createdAt", 1}},
			Options: options.Index().
				SetName("room_created_at"),
		},
		{
			Keys: bson.D{{"expiresAt", 1}},
			Options: options.Index().
				SetName("expires_at"),
		},
		{
			Keys: bson.D{{"roomId", 1}, {"deliveryStatus.userId", 1}, {"deliveryStatus.status", 1}, {"createdAt", 1}},
			Options: options.Index().
				SetName("room_recipient_status"),
		},
	})
	return err
}

func (s *mongoMessageStore) Close(ctx context.Context) error {
	if s == nil || s.client == nil {
		return nil
	}
	return s.client.Disconnect(ctx)
}

func (s *mongoMessageStore) SaveMessage(ctx context.Context, msg *Message) error {
	if s == nil {
		return errors.New("message store is not configured")
	}
	doc := messageToStored(msg)
	_, err := s.collection.UpdateOne(
		ctx,
		bson.M{"roomId": msg.RoomID, "messageId": msg.MessageID},
		bson.M{"$setOnInsert": doc},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func (s *mongoMessageStore) RoomMessages(ctx context.Context, roomID string, before int64, after int64, afterID string, limit int) ([]*Message, bool, error) {
	if s == nil {
		return nil, false, errors.New("message store is not configured")
	}
	if limit <= 0 {
		limit = defaultSyncLimit
	}
	if limit > defaultSyncLimit {
		limit = defaultSyncLimit
	}
	now := time.Now().UTC().Unix()
	filter := bson.M{
		"roomId":    roomID,
		"expiresAt": bson.M{"$gt": now},
	}
	sortOrder := bson.D{{"createdAt", -1}, {"messageId", -1}}
	reverseResults := true
	if before > 0 {
		filter["createdAt"] = bson.M{"$lt": before}
	} else if after > 0 {
		if afterID != "" {
			filter["$or"] = bson.A{
				bson.M{"createdAt": bson.M{"$gt": after}},
				bson.M{"createdAt": after, "messageId": bson.M{"$gt": afterID}},
			}
		} else {
			filter["createdAt"] = bson.M{"$gt": after}
		}
		sortOrder = bson.D{{"createdAt", 1}, {"messageId", 1}}
		reverseResults = false
	}
	cursor, err := s.collection.Find(ctx, filter, options.Find().
		SetSort(sortOrder).
		SetLimit(int64(limit+1)),
	)
	if err != nil {
		return nil, false, err
	}
	defer cursor.Close(ctx)

	var stored []storedMessage
	if err := cursor.All(ctx, &stored); err != nil {
		return nil, false, err
	}
	hasMore := len(stored) > limit
	if hasMore {
		stored = stored[:limit]
	}
	out := make([]*Message, 0, len(stored))
	if reverseResults {
		for i := len(stored) - 1; i >= 0; i-- {
			msg := storedToMessage(stored[i])
			out = append(out, &msg)
		}
	} else {
		for i := range stored {
			msg := storedToMessage(stored[i])
			out = append(out, &msg)
		}
	}
	return out, hasMore, nil
}

func (s *mongoMessageStore) MissedMessages(ctx context.Context, roomID, userID string, limit int) ([]*Message, error) {
	if s == nil {
		return nil, errors.New("message store is not configured")
	}
	if limit <= 0 {
		limit = defaultSyncLimit
	}
	now := time.Now().UTC().Unix()
	filter := bson.M{
		"roomId":    roomID,
		"senderId":  bson.M{"$ne": userID},
		"expiresAt": bson.M{"$gt": now},
		"deliveryStatus": bson.M{
			"$elemMatch": bson.M{
				"userId": userID,
				"status": messageStatusPending,
			},
		},
	}
	cursor, err := s.collection.Find(ctx, filter, options.Find().
		SetSort(bson.D{{"createdAt", 1}, {"messageId", 1}}).
		SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var stored []storedMessage
	if err := cursor.All(ctx, &stored); err != nil {
		return nil, err
	}
	out := make([]*Message, 0, len(stored))
	for i := range stored {
		msg := storedToMessage(stored[i])
		out = append(out, &msg)
	}
	return out, nil
}

func (s *mongoMessageStore) MarkStatus(ctx context.Context, roomID, messageID, userID, status string) (*Message, error) {
	if s == nil {
		return nil, errors.New("message store is not configured")
	}
	now := time.Now().UTC().Unix()
	statusMatch := bson.M{"userId": userID}
	if status != messageStatusRead {
		statusMatch["status"] = bson.M{"$ne": messageStatusRead}
	}
	result := s.collection.FindOneAndUpdate(
		ctx,
		bson.M{
			"roomId":         roomID,
			"messageId":      messageID,
			"deliveryStatus": bson.M{"$elemMatch": statusMatch},
		},
		bson.M{"$set": bson.M{
			"deliveryStatus.$.status":    status,
			"deliveryStatus.$.updatedAt": now,
			"updatedAt":                  now,
		}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	)
	var stored storedMessage
	if err := result.Decode(&stored); err != nil {
		return nil, err
	}
	msg := storedToMessage(stored)
	return &msg, nil
}

func (s *mongoMessageStore) DeleteExpired(ctx context.Context, now int64) error {
	if s == nil {
		return nil
	}
	_, err := s.collection.DeleteMany(ctx, bson.M{"expiresAt": bson.M{"$lte": now}})
	return err
}

func messageToStored(msg *Message) storedMessage {
	statuses := make([]storedDeliveryStatus, 0, len(msg.DeliveryStatus))
	for userID, status := range msg.DeliveryStatus {
		statuses = append(statuses, storedDeliveryStatus{
			UserID:    userID,
			Status:    status,
			UpdatedAt: msg.UpdatedAt,
		})
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].UserID < statuses[j].UserID
	})
	return storedMessage{
		MessageID:      msg.MessageID,
		RoomID:         msg.RoomID,
		SenderID:       msg.SenderID,
		Text:           msg.Text,
		CreatedAt:      msg.CreatedAt,
		UpdatedAt:      msg.UpdatedAt,
		ExpiresAt:      msg.ExpiresAt,
		DeliveryStatus: statuses,
	}
}

func storedToMessage(stored storedMessage) Message {
	statuses := make(map[string]string, len(stored.DeliveryStatus))
	pending := make(map[string]bool)
	for _, item := range stored.DeliveryStatus {
		if item.UserID == "" {
			continue
		}
		statuses[item.UserID] = item.Status
		if statusNeedsAck(item.Status) {
			pending[item.UserID] = true
		}
	}
	return Message{
		MessageID:      stored.MessageID,
		RoomID:         stored.RoomID,
		SenderID:       stored.SenderID,
		Text:           stored.Text,
		CreatedAt:      stored.CreatedAt,
		UpdatedAt:      stored.UpdatedAt,
		ExpiresAt:      stored.ExpiresAt,
		DeliveryStatus: statuses,
		PendingAcks:    pending,
	}
}

func (s *RelayServer) closeMessageStore() {
	if s.messageStore == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.messageStore.Close(ctx); err != nil {
		log.Printf("MongoDB message store close error: %v", err)
	}
}

func main() {
	log.Println("Program started")
	configPath := flag.String("config", "relay_config.json", "path to relay config json")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	srv := &RelayServer{
		relayID:            "",
		relayName:          cfg.RelayName,
		publicPort:         cfg.PublicPort,
		region:             cfg.Region,
		publicURL:          cfg.PublicURL,
		useCloudflare:      cfg.UseCloudflare,
		registryURL:        strings.TrimRight(strings.TrimSpace(cfg.RegistryURL), "/"),
		httpClient:         &http.Client{Timeout: 5 * time.Second},
		startedAt:          time.Now().UTC(),
		heartbeatEvery:     time.Duration(cfg.HeartbeatSeconds) * time.Second,
		roomTTL:            time.Duration(cfg.RoomTTLSeconds) * time.Second,
		messageTTL:         time.Duration(cfg.MessageTTLSeconds) * time.Second,
		offlineTTL:         time.Duration(cfg.OfflineMessageRetentionSeconds) * time.Second,
		allowNewRooms:      cfg.AllowNewRooms,
		maxRooms:           cfg.MaxRooms,
		maxUsers:           cfg.MaxUsers,
		roomMaxUsers:       cfg.RoomMaxUsers,
		offlineDefault:     cfg.OfflineMessagesEnabled,
		messageSyncLimit:   cfg.MessageSyncLimit,
		rooms:              make(map[string]*Room),
		cloudflareDone:     make(chan struct{}),
		cloudflareURLReady: make(chan struct{}),
	}

	if strings.TrimSpace(srv.relayName) == "" {
		srv.relayName = "Relay"
	}
	if srv.publicPort <= 0 {
		srv.publicPort = 9000
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
	if srv.offlineTTL <= 0 {
		srv.offlineTTL = defaultOfflineTTL
	}
	if srv.messageSyncLimit <= 0 {
		srv.messageSyncLimit = defaultSyncLimit
	}
	if srv.offlineDefault && strings.TrimSpace(cfg.MongoURI) == "" {
		log.Fatalf("offline messages are enabled, but mongoUri is empty")
	}
	if strings.TrimSpace(cfg.MongoURI) != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		store, err := newMongoMessageStore(ctx, cfg)
		cancel()
		if err != nil {
			log.Fatalf("connect MongoDB message store: %v", err)
		}
		srv.messageStore = store
		defer srv.closeMessageStore()
		log.Printf("MongoDB offline message store enabled: database=%s collection=%s", cfg.MongoDatabase, cfg.MongoMessagesCollection)
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

	defer srv.stopCloudflared()
	log.Printf("relay server listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("relay server error: %v", err)
	}
}

func getenv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvBool(key string, def bool) bool {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func loadConfig(path string) (*Config, error) {
	cfg := Config{}

	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}

	cfg.RelayName = strings.TrimSpace(cfg.RelayName)
	cfg.MongoURI = strings.TrimSpace(cfg.MongoURI)
	cfg.MongoDatabase = strings.TrimSpace(cfg.MongoDatabase)
	cfg.MongoMessagesCollection = strings.TrimSpace(cfg.MongoMessagesCollection)
	cfg.RegistryURL = strings.TrimSpace(cfg.RegistryURL)
	cfg.Region = strings.TrimSpace(cfg.Region)
	cfg.RelayName = getenv("RELAY_NAME", cfg.RelayName)

	cfg.PublicPort = getenvInt("PUBLIC_PORT", cfg.PublicPort)

	cfg.PublicURL = getenv("PUBLIC_URL", cfg.PublicURL)

	cfg.UseCloudflare = getenvBool(
		"USE_CLOUDFLARE",
		cfg.UseCloudflare,
	)

	cfg.RegistryURL = getenv(
		"REGISTRY_URL",
		cfg.RegistryURL,
	)

	cfg.Region = getenv(
		"REGION",
		cfg.Region,
	)

	cfg.MaxRooms = getenvInt(
		"MAX_ROOMS",
		cfg.MaxRooms,
	)

	cfg.MaxUsers = getenvInt(
		"MAX_USERS",
		cfg.MaxUsers,
	)

	cfg.RoomMaxUsers = getenvInt(
		"ROOM_MAX_USERS",
		cfg.RoomMaxUsers,
	)

	cfg.AllowNewRooms = getenvBool(
		"ALLOW_NEW_ROOMS",
		cfg.AllowNewRooms,
	)

	cfg.HeartbeatSeconds = getenvInt(
		"HEARTBEAT_SECONDS",
		cfg.HeartbeatSeconds,
	)

	cfg.RoomTTLSeconds = getenvInt(
		"ROOM_TTL_SECONDS",
		cfg.RoomTTLSeconds,
	)

	cfg.MessageTTLSeconds = getenvInt(
		"MESSAGE_TTL_SECONDS",
		cfg.MessageTTLSeconds,
	)
	cfg.OfflineMessagesEnabled = getenvBool(
		"OFFLINE_MESSAGES_ENABLED",
		cfg.OfflineMessagesEnabled,
	)
	cfg.MongoURI = getenv(
		"MONGO_URI",
		cfg.MongoURI,
	)
	cfg.MongoDatabase = getenv(
		"MONGO_DATABASE",
		cfg.MongoDatabase,
	)
	cfg.MongoMessagesCollection = getenv(
		"MONGO_MESSAGES_COLLECTION",
		cfg.MongoMessagesCollection,
	)
	cfg.OfflineMessageRetentionSeconds = getenvInt(
		"OFFLINE_MESSAGE_RETENTION_SECONDS",
		cfg.OfflineMessageRetentionSeconds,
	)
	cfg.MessageSyncLimit = getenvInt(
		"MESSAGE_SYNC_LIMIT",
		cfg.MessageSyncLimit,
	)
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
	if cfg.MongoDatabase == "" {
		cfg.MongoDatabase = defaultMongoDatabase
	}
	if cfg.MongoMessagesCollection == "" {
		cfg.MongoMessagesCollection = defaultMongoCollection
	}
	if cfg.OfflineMessageRetentionSeconds <= 0 {
		cfg.OfflineMessageRetentionSeconds = int(defaultOfflineTTL / time.Second)
	}
	if cfg.MessageSyncLimit <= 0 {
		cfg.MessageSyncLimit = defaultSyncLimit
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
		"ok":                       true,
		"relayId":                  s.relayID,
		"relayName":                s.relayName,
		"publicPort":               s.publicPort,
		"publicURL":                s.publicURL,
		"useCloudflare":            s.useCloudflare,
		"region":                   s.region,
		"rooms":                    rooms,
		"users":                    users,
		"startedAt":                s.startedAt.Unix(),
		"offlineMessagesEnabled":   s.offlineDefault,
		"offlineMessageStoreReady": s.messageStore != nil,
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
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/rooms/"), "/")
	parts := strings.Split(path, "/")
	roomID := ""
	if len(parts) > 0 {
		roomID = strings.TrimSpace(parts[0])
	}
	if roomID == "" {
		writeJSONError(w, http.StatusBadRequest, "room_id_required")
		return
	}
	if len(parts) == 2 && parts[1] == "messages" {
		s.handleRoomMessages(w, r, roomID)
		return
	}
	if len(parts) != 1 {
		writeJSONError(w, http.StatusNotFound, "route_not_found")
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

func (s *RelayServer) handleRoomMessages(w http.ResponseWriter, r *http.Request, roomID string) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	room, ok := s.getRoom(roomID)
	if !ok {
		writeJSONError(w, http.StatusNotFound, "room_not_found")
		return
	}
	snap := room.snapshot()
	if !snap.OfflineMessagesEnabled {
		writeJSON(w, http.StatusOK, messageHistoryResponse{RoomID: roomID, Messages: []persistedMessageResponse{}, HasMore: false})
		return
	}
	if s.messageStore == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "offline_message_store_unavailable")
		return
	}

	before := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("before")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid_before")
			return
		}
		before = parsed
	}
	after := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("after")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid_after")
			return
		}
		after = parsed
	}
	afterID := strings.TrimSpace(r.URL.Query().Get("afterId"))
	if before > 0 && after > 0 {
		writeJSONError(w, http.StatusBadRequest, "before_after_conflict")
		return
	}
	limit := defaultSyncLimit
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			writeJSONError(w, http.StatusBadRequest, "invalid_limit")
			return
		}
		limit = parsed
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	messages, hasMore, err := s.messageStore.RoomMessages(ctx, roomID, before, after, afterID, limit)
	cancel()
	if err != nil {
		log.Printf("list room messages failed: room=%s error=%v", roomID, err)
		writeJSONError(w, http.StatusInternalServerError, "message_history_failed")
		return
	}
	out := make([]persistedMessageResponse, 0, len(messages))
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		out = append(out, persistedMessageResponse{
			MessageID:      msg.MessageID,
			RoomID:         msg.RoomID,
			SenderID:       msg.SenderID,
			Text:           msg.Text,
			CreatedAt:      msg.CreatedAt,
			UpdatedAt:      msg.UpdatedAt,
			DeliveryStatus: cloneDeliveryStatus(msg.DeliveryStatus),
		})
	}
	writeJSON(w, http.StatusOK, messageHistoryResponse{RoomID: roomID, Messages: out, HasMore: hasMore})
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
	if req.OfflineMessagesEnabled != nil && *req.OfflineMessagesEnabled && s.messageStore == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "offline_message_store_unavailable")
		return
	}
	room := s.ensureRoom(req.RoomID, req.PinHash, req.MaxUsers, req.OfflineMessagesEnabled)

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
	var req struct {
		RoomID string `json:"roomId"`
	}
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
		Type:                   "room_joined",
		Ok:                     true,
		Room:                   &snap,
		Users:                  room.connectedMembersSnapshot(),
		UserID:                 req.UserID,
		RoomID:                 req.RoomID,
		OfflineMessagesEnabled: snap.OfflineMessagesEnabled,
	})

	room.broadcastExcept(req.UserID, wsEnvelope{
		Type:      "user_joined",
		RoomID:    room.RoomID,
		UserID:    req.UserID,
		Nickname:  req.Nickname,
		Transport: req.Transport,
		PeerInfo:  req.PeerInfo,
	})

	c.syncMissedMessages()
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

func (c *Client) syncMissedMessages() {
	if c.room == nil || c.UserID == "" {
		return
	}
	room := c.room
	room.mu.RLock()
	roomID := room.RoomID
	offlineEnabled := room.OfflineMessagesEnabled
	room.mu.RUnlock()
	if !offlineEnabled {
		return
	}
	if c.srv.messageStore == nil {
		c.writeError("offline_message_store_unavailable")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	missed, err := c.srv.messageStore.MissedMessages(ctx, roomID, c.UserID, c.srv.messageSyncLimit)
	cancel()
	if err != nil {
		log.Printf("sync missed messages failed: room=%s user=%s error=%v", roomID, c.UserID, err)
		c.writeError("message_sync_failed")
		return
	}

	for _, msg := range missed {
		if msg == nil {
			continue
		}
		if statusCanAdvance(msg.DeliveryStatus[c.UserID], messageStatusSent) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			updated, err := c.srv.messageStore.MarkStatus(ctx, roomID, msg.MessageID, c.UserID, messageStatusSent)
			cancel()
			if err != nil {
				if errors.Is(err, mongo.ErrNoDocuments) {
					continue
				}
				log.Printf("mark missed message sent failed: room=%s message=%s user=%s error=%v", roomID, msg.MessageID, c.UserID, err)
				continue
			}
			msg = updated
		}

		room.mu.Lock()
		if len(msg.PendingAcks) > 0 {
			room.Messages[msg.MessageID] = msg
		}
		room.mu.Unlock()

		c.sendJSON(messageEnvelope(msg, c.UserID))
	}
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
	offlineEnabled := room.OfflineMessagesEnabled
	if offlineEnabled && c.srv.messageStore == nil {
		room.mu.Unlock()
		c.writeError("offline_message_store_unavailable")
		return
	}

	msg := &Message{
		MessageID:      msgID,
		RoomID:         room.RoomID,
		SenderID:       c.UserID,
		Text:           text,
		CreatedAt:      now,
		UpdatedAt:      now,
		ExpiresAt:      now + int64(c.srv.messageTTL.Seconds()),
		DeliveryStatus: make(map[string]string),
		PendingAcks:    make(map[string]bool),
	}
	if offlineEnabled {
		msg.ExpiresAt = now + int64(c.srv.offlineTTL.Seconds())
	}

	targets := make([]*Client, 0, len(room.Clients))
	for userID := range room.Members {
		if userID == c.UserID {
			continue
		}
		if client, ok := room.Clients[userID]; ok && client != nil {
			msg.DeliveryStatus[userID] = messageStatusSent
			msg.PendingAcks[userID] = true
			targets = append(targets, client)
			continue
		}
		if offlineEnabled {
			msg.DeliveryStatus[userID] = messageStatusPending
			msg.PendingAcks[userID] = true
		}
	}

	if len(msg.PendingAcks) > 0 {
		room.Messages[msgID] = msg
	}
	room.LastActivityAt = now
	room.UpdatedAt = now
	roomID := room.RoomID
	deliveryStatus := cloneDeliveryStatus(msg.DeliveryStatus)
	room.mu.Unlock()

	if offlineEnabled {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := c.srv.messageStore.SaveMessage(ctx, msg)
		cancel()
		if err != nil {
			room.mu.Lock()
			delete(room.Messages, msgID)
			room.mu.Unlock()
			log.Printf("save offline message failed: room=%s message=%s error=%v", roomID, msgID, err)
			c.writeError("offline_message_save_failed")
			return
		}
	}

	for _, target := range targets {
		target.sendJSON(messageEnvelope(msg, target.UserID))
	}

	c.sendJSON(wsEnvelope{
		Type:           "message_accepted",
		Ok:             true,
		RoomID:         roomID,
		MessageID:      msgID,
		SenderID:       c.UserID,
		CreatedAt:      msg.CreatedAt,
		UpdatedAt:      msg.UpdatedAt,
		DeliveryStatus: deliveryStatus,
	})
}

func (c *Client) handleAck(req ackRequest) {
	if c.room == nil || c.UserID == "" {
		return
	}
	messageID := strings.TrimSpace(req.MessageID)
	if messageID == "" {
		c.writeError("message_id_required")
		return
	}
	status, ok := normalizeAckStatus(req.Status)
	if !ok {
		c.writeError("invalid_ack_status")
		return
	}

	room := c.room
	room.mu.RLock()
	roomID := room.RoomID
	offlineEnabled := room.OfflineMessagesEnabled
	room.mu.RUnlock()

	var stored *Message
	if offlineEnabled && c.srv.messageStore != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		msg, err := c.srv.messageStore.MarkStatus(ctx, roomID, messageID, c.UserID, status)
		cancel()
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			log.Printf("ack store update failed: room=%s message=%s user=%s status=%s error=%v", roomID, messageID, c.UserID, status, err)
			c.writeError("ack_store_failed")
			return
		}
		stored = msg
	}

	updatedAt := time.Now().UTC().Unix()
	effectiveStatus := status
	room.mu.Lock()
	msg, found := room.Messages[messageID]
	if found && msg.DeliveryStatus == nil {
		msg.DeliveryStatus = make(map[string]string)
	}
	if found && statusCanAdvance(msg.DeliveryStatus[c.UserID], status) {
		msg.DeliveryStatus[c.UserID] = status
		msg.UpdatedAt = updatedAt
	} else if found && msg.DeliveryStatus[c.UserID] != "" {
		effectiveStatus = msg.DeliveryStatus[c.UserID]
	}
	if found && !statusNeedsAck(status) {
		delete(msg.PendingAcks, c.UserID)
		room.removeFromPostboxLocked(c.UserID, msg.MessageID)
	}
	if found && len(msg.PendingAcks) == 0 {
		delete(room.Messages, msg.MessageID)
		room.removeFromAllPostboxesLocked(msg.MessageID)
	}
	if !found && stored != nil {
		msg = stored
		found = true
		if len(stored.PendingAcks) > 0 {
			room.Messages[stored.MessageID] = stored
		}
	}
	var sender *Client
	var deliveryStatus map[string]string
	if stored != nil {
		deliveryStatus = cloneDeliveryStatus(stored.DeliveryStatus)
		sender = room.Clients[stored.SenderID]
		updatedAt = stored.UpdatedAt
		if stored.DeliveryStatus[c.UserID] != "" {
			effectiveStatus = stored.DeliveryStatus[c.UserID]
		}
	} else if found {
		deliveryStatus = cloneDeliveryStatus(msg.DeliveryStatus)
		sender = room.Clients[msg.SenderID]
	}
	knownMessage := found || stored != nil
	room.mu.Unlock()
	if !knownMessage {
		return
	}

	c.sendJSON(wsEnvelope{
		Type:      "ack_accepted",
		Ok:        true,
		RoomID:    roomID,
		MessageID: messageID,
		Status:    effectiveStatus,
		UpdatedAt: updatedAt,
	})

	if sender != nil {
		sender.sendJSON(wsEnvelope{
			Type:           "message_status",
			RoomID:         roomID,
			MessageID:      messageID,
			UserID:         c.UserID,
			Status:         effectiveStatus,
			DeliveryStatus: deliveryStatus,
			UpdatedAt:      updatedAt,
		})
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
	log.Println("Registering with registry")
	if s.useCloudflare && strings.TrimSpace(s.publicURL) == "" {
		url, err := s.startCloudflareTunnel()
		if err != nil {
			return err
		}
		s.publicURL = url
		log.Printf("Cloudflare Tunnel started: %s", s.publicURL)
	}
	payload := map[string]any{
		"relayId":                  s.relayID,
		"relayName":                s.relayName,
		"publicPort":               s.publicPort,
		"publicURL":                s.publicURL,
		"region":                   s.region,
		"offlineMessagesEnabled":   s.offlineDefault,
		"offlineMessagesSupported": s.messageStore != nil,
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
	log.Println("Registry registered")
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
			"relayId":                  s.relayID,
			"currentRooms":             rooms,
			"currentUsers":             users,
			"region":                   s.region,
			"offlineMessagesEnabled":   s.offlineDefault,
			"offlineMessagesSupported": s.messageStore != nil,
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
		if s.messageStore != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := s.messageStore.DeleteExpired(ctx, now); err != nil {
				log.Printf("delete expired offline messages failed: %v", err)
			}
			cancel()
		}
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

func (s *RelayServer) ensureRoom(roomID, pinHash string, maxUsers int, offlineMessagesEnabled *bool) *Room {
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
		if offlineMessagesEnabled != nil {
			room.OfflineMessagesEnabled = *offlineMessagesEnabled
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
		RoomID:                 roomID,
		PinHash:                pinHash,
		MaxUsers:               maxUsers,
		CreatedAt:              now,
		UpdatedAt:              now,
		LastActivityAt:         now,
		OfflineMessagesEnabled: s.offlineDefault,
		Clients:                make(map[string]*Client),
		Members:                make(map[string]*Member),
		Messages:               make(map[string]*Message),
		Postboxes:              make(map[string][]*Message),
	}
	if offlineMessagesEnabled != nil {
		room.OfflineMessagesEnabled = *offlineMessagesEnabled
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
		RoomID:                 room.RoomID,
		MaxUsers:               room.MaxUsers,
		ConnectedUsers:         connected,
		MemberCount:            len(room.Members),
		MessageCount:           len(room.Messages),
		CreatedAt:              room.CreatedAt,
		UpdatedAt:              room.UpdatedAt,
		LastActivityAt:         room.LastActivityAt,
		OfflineMessagesEnabled: room.OfflineMessagesEnabled,
		Users:                  users,
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

func messageEnvelope(msg *Message, recipientID string) wsEnvelope {
	status := ""
	if msg.DeliveryStatus != nil {
		status = msg.DeliveryStatus[recipientID]
	}
	return wsEnvelope{
		Type:      "message",
		RoomID:    msg.RoomID,
		MessageID: msg.MessageID,
		SenderID:  msg.SenderID,
		Text:      msg.Text,
		CreatedAt: msg.CreatedAt,
		UpdatedAt: msg.UpdatedAt,
		Status:    status,
	}
}

func cloneDeliveryStatus(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for userID, status := range in {
		out[userID] = status
	}
	return out
}

func normalizeAckStatus(status string) (string, bool) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "":
		return messageStatusDelivered, true
	case messageStatusDelivered:
		return messageStatusDelivered, true
	case messageStatusRead:
		return messageStatusRead, true
	default:
		return "", false
	}
}

func statusNeedsAck(status string) bool {
	return status == messageStatusPending || status == messageStatusSent
}

func statusRank(status string) int {
	switch status {
	case messageStatusPending:
		return 0
	case messageStatusSent:
		return 1
	case messageStatusDelivered:
		return 2
	case messageStatusRead:
		return 3
	default:
		return -1
	}
}

func statusCanAdvance(current, next string) bool {
	return statusRank(next) >= statusRank(current)
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

func (s *RelayServer) monitorCloudflared() {
	if s.cloudflaredCmd == nil {
		return
	}
	go func(cmd *exec.Cmd) {
		err := cmd.Wait()
		select {
		case <-s.cloudflareDone:
			return
		default:
		}
		log.Printf("cloudflared exited: %v", err)
		s.publicURL = ""
		s.cloudflaredCmd = nil
		go func() {
			if _, err := s.startCloudflareTunnel(); err != nil {
				log.Printf("cloudflared restart failed: %v", err)
			} else {
				log.Printf("cloudflared restarted")
			}
		}()
	}(s.cloudflaredCmd)
}

func (s *RelayServer) stopCloudflared() {
	s.cloudflareMu.Lock()
	defer s.cloudflareMu.Unlock()
	select {
	case <-s.cloudflareDone:
	default:
		close(s.cloudflareDone)
	}
	if s.cloudflaredCmd != nil && s.cloudflaredCmd.Process != nil {
		_ = s.cloudflaredCmd.Process.Signal(os.Interrupt)
		time.Sleep(2 * time.Second)
		_ = s.cloudflaredCmd.Process.Kill()
	}
}

func (s *RelayServer) startCloudflareTunnel() (string, error) {
	s.cloudflareMu.Lock()
	defer s.cloudflareMu.Unlock()

	if strings.TrimSpace(s.publicURL) != "" {
		return s.publicURL, nil
	}

	cmd := exec.Command(
		"./cloudflared",
		"tunnel",
		"--url",
		fmt.Sprintf("http://localhost:%d", s.publicPort),
		"--output",
		"json",
	)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return "", err
	}

	if err := cmd.Start(); err != nil {
		return "", err
	}

	log.Printf("Started cloudflared (pid=%d)", cmd.Process.Pid)
	s.cloudflaredCmd = cmd
	s.monitorCloudflared()

	type cloudflaredLog struct {
		Level   string `json:"level"`
		Message string `json:"message"`
	}

	scanner := bufio.NewScanner(stderr)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()

	for {
		select {
		case <-timeout.C:
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
			return "", fmt.Errorf("timed out waiting for Cloudflare Tunnel URL")
		default:
		}

		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
				return "", err
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}

		var entry cloudflaredLog
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}

		log.Println(entry.Message)

		fields := strings.Fields(entry.Message)
		for _, f := range fields {
			if strings.HasPrefix(f, "https://") && strings.Contains(f, ".trycloudflare.com") {
				url := strings.Trim(f, "| ")
				url = strings.TrimPrefix(url, "https://")
				s.publicURL = url
				log.Printf("Cloudflare Tunnel URL: %s", url)
				if s.useCloudflare {
					return url, nil
				}
				go func() {
					for scanner.Scan() {
						var e cloudflaredLog
						if json.Unmarshal(scanner.Bytes(), &e) == nil {
							log.Println(e.Message)
						}
					}
				}()
				return url, nil
			}
		}
	}
}
