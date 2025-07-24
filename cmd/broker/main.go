package main

import (
    "context"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"

    "github.com/gorilla/websocket"
)

// --- Configuration ---
var (
    retentionTime = flag.Duration("retention", 24*time.Hour, "Message retention duration")
    maxMessages   = flag.Int("max-messages", 1000, "Maximum messages per topic")
    serverAddr    = flag.String("addr", ":8080", "Server address")
)

// --- WebSocket Setup ---
var upgrader = websocket.Upgrader{
    CheckOrigin: func(r *http.Request) bool { return true },
}

// --- Data Structures ---
type Client struct {
    ID       string
    Conn     *websocket.Conn
    Topic    string
    Group    string
    Offset   int64
    Pending  map[int]Message // unacknowledged messages
    mu       sync.Mutex      // protects Pending map
}

type Message struct {
    ID        int    `json:"id"`
    Topic     string `json:"topic"`
    Content   string `json:"content"`
    Timestamp string `json:"timestamp"`
    Ack       bool   `json:"ack"`
    Key       string `json:"key,omitempty"` // for partitioning/compaction
}

// --- Global State ---
var (
    clients      = make(map[*Client]bool)
    topics       = make(map[string][]Message)
    clientsLock  sync.RWMutex
    topicsLock   sync.RWMutex
    server       *http.Server
    messageCounter = make(map[string]int) // per-topic message ID counter
    counterLock  sync.Mutex
)

// --- Main ---
func main() {
    flag.Parse()

    mux := http.NewServeMux()
    mux.HandleFunc("/ws", handleWebSocket)
    mux.HandleFunc("/topics", handleTopicList)
    mux.HandleFunc("/health", handleHealthCheck)

    server = &http.Server{
        Addr:    *serverAddr,
        Handler: mux,
    }

    // Start server in goroutine
    go func() {
        fmt.Printf("☕ Swipe Kafei v5 Broker running on http://localhost%s/ws\n", *serverAddr)
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatalf("Server failed: %v", err)
        }
    }()

    // Start cleanup goroutine for expired messages
    go cleanupExpiredMessages()

    // Wait for interrupt signal
    c := make(chan os.Signal, 1)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    <-c

    // Graceful shutdown
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    if err := server.Shutdown(ctx); err != nil {
        log.Printf("Server shutdown error: %v", err)
    }
    log.Println("Broker shut down gracefully.")
}

// --- WebSocket Handler ---
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        log.Println("WS upgrade error:", err)
        return
    }
    defer conn.Close()

    // First message = subscription request
    _, msgBytes, err := conn.ReadMessage()
    if err != nil {
        log.Println("Failed to read subscription:", err)
        return
    }

    var subReq Message
    if err := json.Unmarshal(msgBytes, &subReq); err != nil {
        log.Println("Invalid subscription format")
        return
    }

    client := &Client{
        ID:      fmt.Sprintf("client-%d", time.Now().UnixNano()),
        Conn:    conn,
        Topic:   subReq.Topic,
        Group:   subReq.Key, // Use key as group identifier
        Offset:  0,
        Pending: make(map[int]Message),
    }

    clientsLock.Lock()
    clients[client] = true
    clientsLock.Unlock()

    log.Printf("✅ Client subscribed to '%s' [%s] in group '%s'\n", client.Topic, client.ID, client.Group)

    // Send previous messages from topic
    sendHistory(client)

    // Read incoming messages
    for {
        _, rawMsg, err := conn.ReadMessage()
        if err != nil {
            log.Println("❌ Client disconnected:", client.ID)
            cleanupClient(client)
            break
        }

        var msg Message
        if err := json.Unmarshal(rawMsg, &msg); err != nil {
            log.Println("⚠️ Bad message format")
            continue
        }

        // Handle ACK
        if msg.Ack {
            handleAck(client, msg.ID)
            continue
        }

        // Handle new message
        msg.ID = generateMessageID(msg.Topic)
        msg.Timestamp = time.Now().Format(time.RFC3339)

        storeMessage(msg)
        go broadcastToTopic(msg.Topic, msg)
    }
}

// --- Store Message in Memory (with limit and retention) ---
func storeMessage(msg Message) {
    topicsLock.Lock()
    defer topicsLock.Unlock()

    queue := topics[msg.Topic]
    
    // Remove expired messages
    now := time.Now()
    cutoff := now.Add(-*retentionTime)
    filtered := make([]Message, 0, len(queue))
    for _, m := range queue {
        ts, _ := time.Parse(time.RFC3339, m.Timestamp)
        if ts.After(cutoff) {
            filtered = append(filtered, m)
        }
    }
    queue = filtered

    // Add new message
    queue = append(queue, msg)
    
    // Trim to max size
    if len(queue) > *maxMessages {
        queue = queue[len(queue)-*maxMessages:]
    }
    
    topics[msg.Topic] = queue
}

// --- Generate Unique Message ID per Topic ---
func generateMessageID(topic string) int {
    counterLock.Lock()
    defer counterLock.Unlock()
    
    messageCounter[topic]++
    return messageCounter[topic]
}

// --- Send History to New Subscriber ---
func sendHistory(c *Client) {
    topicsLock.RLock()
    defer topicsLock.RUnlock()

    if history, ok := topics[c.Topic]; ok {
        for i := range history {
            if int64(history[i].ID) > c.Offset {
                b, err := json.Marshal(history[i])
                if err != nil {
                    log.Printf("Marshal error for client %s: %v", c.ID, err)
                    continue
                }
                if err := c.Conn.WriteMessage(websocket.TextMessage, b); err != nil {
                    log.Printf("Write error to client %s: %v", c.ID, err)
                    return
                }
                // Track as pending
                c.mu.Lock()
                c.Pending[history[i].ID] = history[i]
                c.mu.Unlock()
            }
        }
    }
}

// --- Broadcast Message to Subscribers ---
func broadcastToTopic(topic string, msg Message) {
    b, err := json.Marshal(msg)
    if err != nil {
        log.Printf("Marshal error for message ID %d: %v", msg.ID, err)
        return
    }

    clientsLock.Lock()
    defer clientsLock.Unlock()

    // Group clients by consumer group
    groups := make(map[string][]*Client)
    for client := range clients {
        if client.Topic == topic {
            groups[client.Group] = append(groups[client.Group], client)
        }
    }

    // Distribute to one client per group (round-robin style)
    for groupName, groupClients := range groups {
        if len(groupClients) == 0 {
            continue
        }
        
        // Simple round-robin selection
        selectedClient := groupClients[msg.ID%len(groupClients)]
        
        err := selectedClient.Conn.WriteMessage(websocket.TextMessage, b)
        if err != nil {
            log.Printf("⚠️ Write error to client %s in group %s: %v", selectedClient.ID, groupName, err)
            cleanupClient(selectedClient)
            continue
        }
        
        // Track as pending
        selectedClient.mu.Lock()
        selectedClient.Pending[msg.ID] = msg
        selectedClient.mu.Unlock()
        
        log.Printf("📤 Message %d sent to client %s in group %s", msg.ID, selectedClient.ID, groupName)
    }
}

// --- Handle Message Acknowledgment ---
func handleAck(c *Client, msgID int) {
    c.mu.Lock()
    defer c.mu.Unlock()
    
    delete(c.Pending, msgID)
    c.Offset = int64(msgID)
    log.Printf("✅ ACK received from %s for message ID %d", c.ID, msgID)
}

// --- Cleanup Expired Messages Periodically ---
func cleanupExpiredMessages() {
    ticker := time.NewTicker(10 * time.Minute)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            cleanupExpired()
        }
    }
}

func cleanupExpired() {
    topicsLock.Lock()
    defer topicsLock.Unlock()
    
    now := time.Now()
    cutoff := now.Add(-*retentionTime)
    
    for topic, messages := range topics {
        filtered := make([]Message, 0, len(messages))
        for _, msg := range messages {
            ts, err := time.Parse(time.RFC3339, msg.Timestamp)
            if err != nil || ts.After(cutoff) {
                filtered = append(filtered, msg)
            }
        }
        topics[topic] = filtered
    }
    
    log.Println("🧹 Expired messages cleaned up")
}

// --- List Active Topics (REST endpoint) ---
func handleTopicList(w http.ResponseWriter, r *http.Request) {
    topicsLock.RLock()
    defer topicsLock.RUnlock()

    var topicList []string
    for topic := range topics {
        topicList = append(topicList, topic)
    }

    resp, err := json.Marshal(topicList)
    if err != nil {
        http.Error(w, "Internal server error", http.StatusInternalServerError)
        return
    }
    
    w.Header().Set("Content-Type", "application/json")
    w.Write(resp)
}

// --- Health Check Endpoint ---
func handleHealthCheck(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]string{
        "status": "healthy",
        "uptime": time.Now().Format(time.RFC3339),
    })
}

// --- Cleanup Client Resources ---
func cleanupClient(c *Client) {
    clientsLock.Lock()
    defer clientsLock.Unlock()
    
    if _, exists := clients[c]; exists {
        c.Conn.Close()
        delete(clients, c)
        
        // Handle unacknowledged messages (could requeue or DLQ)
        c.mu.Lock()
        if len(c.Pending) > 0 {
            log.Printf("⚠️ Client %s had %d unacknowledged messages", c.ID, len(c.Pending))
            // In production: requeue or send to DLQ
        }
        c.mu.Unlock()
    }
}
