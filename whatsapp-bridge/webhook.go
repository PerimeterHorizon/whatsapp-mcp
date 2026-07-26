package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func init() {
	// A process restart must resume pending evidence without waiting for another message to arrive.
	// Unit-test binaries use isolated outboxes explicitly and must not touch the live store.
	if strings.HasSuffix(os.Args[0], ".test") {
		return
	}
	if _, err := productionWebhookOutbox(); err != nil {
		fmt.Printf("CRITICAL: webhook outbox did not start: %v\n", err)
	}
}

// WebhookPayload represents the source observation sent to OpenClaw.
type WebhookPayload struct {
	MessageID       string `json:"messageId,omitempty"`
	Sender          string `json:"sender"`
	Content         string `json:"content"`
	ChatJID         string `json:"chatJID"`
	IsFromMe        bool   `json:"isFromMe"`
	MediaType       string `json:"mediaType,omitempty"`
	Filename        string `json:"filename,omitempty"`
	QuotedMessageId string `json:"quotedMessageId,omitempty"`
	QuotedSender    string `json:"quotedSender,omitempty"`
	QuotedContent   string `json:"quotedContent,omitempty"`
}

type webhookOutbox struct {
	db      *sql.DB
	url     string
	secret  string
	client  *http.Client
	wake    chan struct{}
	started sync.Once
}

var (
	defaultOutboxOnce sync.Once
	defaultOutbox     *webhookOutbox
	defaultOutboxErr  error
)

func openWebhookOutbox(path, url, secret string) (*webhookOutbox, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create webhook outbox directory: %w", err)
	}
	db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("open webhook outbox: %w", err)
	}
	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS webhook_outbox (
			message_id TEXT NOT NULL,
			chat_jid TEXT NOT NULL,
			payload BLOB NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
			delivered_at INTEGER,
			PRIMARY KEY (message_id, chat_jid)
		)`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize webhook outbox: %w", err)
	}
	return &webhookOutbox{
		db:     db,
		url:    url,
		secret: secret,
		client: &http.Client{Timeout: 20 * time.Second},
		wake:   make(chan struct{}, 1),
	}, nil
}

func productionWebhookOutbox() (*webhookOutbox, error) {
	defaultOutboxOnce.Do(func() {
		url := os.Getenv("WEBHOOK_URL")
		if url == "" {
			url = "http://localhost:8769/whatsapp/webhook"
		}
		path := os.Getenv("WEBHOOK_OUTBOX_PATH")
		if path == "" {
			path = filepath.Join("store", "webhook-outbox.db")
		}
		defaultOutbox, defaultOutboxErr = openWebhookOutbox(path, url, os.Getenv("WEBHOOK_SECRET"))
		if defaultOutboxErr == nil {
			defaultOutbox.start()
		}
	})
	return defaultOutbox, defaultOutboxErr
}

func (o *webhookOutbox) start() {
	o.started.Do(func() {
		go func() {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-o.wake:
				case <-ticker.C:
				}
				for {
					delivered, err := o.deliverOne()
					if err != nil {
						fmt.Printf("Webhook outbox worker error: %v\n", err)
						break
					}
					if !delivered {
						break
					}
				}
			}
		}()
	})
	o.signal()
}

func (o *webhookOutbox) signal() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *webhookOutbox) enqueue(payload WebhookPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook payload: %w", err)
	}
	now := time.Now().Unix()
	_, err = o.db.Exec(`
		INSERT INTO webhook_outbox
			(message_id, chat_jid, payload, status, attempts, next_attempt_at, created_at, updated_at)
		VALUES (?, ?, ?, 'pending', 0, 0, ?, ?)
		ON CONFLICT(message_id, chat_jid) DO UPDATE SET
			payload = excluded.payload,
			status = 'pending',
			next_attempt_at = 0,
			updated_at = excluded.updated_at
		WHERE webhook_outbox.status != 'delivered'`,
		payload.MessageID, payload.ChatJID, body, now, now)
	if err != nil {
		return fmt.Errorf("persist webhook payload: %w", err)
	}
	o.signal()
	return nil
}

func (o *webhookOutbox) deliverOne() (bool, error) {
	now := time.Now().Unix()
	var messageID, chatJID string
	var body []byte
	var attempts int
	err := o.db.QueryRow(`
		SELECT message_id, chat_jid, payload, attempts
		FROM webhook_outbox
		WHERE status = 'pending' AND next_attempt_at <= ?
		ORDER BY created_at, message_id
		LIMIT 1`, now).Scan(&messageID, &chatJID, &body, &attempts)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	deliveryErr := o.post(body)
	if deliveryErr == nil {
		_, err = o.db.Exec(`
			UPDATE webhook_outbox
			SET status = 'delivered', delivered_at = ?, updated_at = ?, last_error = NULL
			WHERE message_id = ? AND chat_jid = ?`, now, now, messageID, chatJID)
		if err == nil {
			fmt.Printf("✓ Webhook delivered for message %s\n", messageID)
		}
		return true, err
	}

	attempts++
	delaySeconds := int64(1 << min(attempts-1, 5))
	_, err = o.db.Exec(`
		UPDATE webhook_outbox
		SET attempts = ?, next_attempt_at = ?, updated_at = ?, last_error = ?
		WHERE message_id = ? AND chat_jid = ?`,
		attempts, now+delaySeconds, now, deliveryErr.Error(), messageID, chatJID)
	if err != nil {
		return false, err
	}
	fmt.Printf("Webhook delivery deferred for message %s: %v\n", messageID, deliveryErr)
	return true, nil
}

func (o *webhookOutbox) post(body []byte) error {
	req, err := http.NewRequest("POST", o.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if o.secret != "" {
		mac := hmac.New(sha256.New, []byte(o.secret))
		_, _ = mac.Write(body)
		req.Header.Set("X-OpenClaw-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}
	resp, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}

// SendWebhook durably records a source observation before attempting delivery.
// Delivery retries are transport work only; they never invoke a paid brain turn.
func SendWebhook(messageID, sender, content, chatJID string, isFromMe bool, mediaType, filename, quotedMessageId, quotedSender, quotedContent string) {
	outbox, err := productionWebhookOutbox()
	if err != nil {
		fmt.Printf("CRITICAL: webhook observation was not queued: %v\n", err)
		return
	}
	err = outbox.enqueue(WebhookPayload{
		MessageID:       messageID,
		Sender:          sender,
		Content:         content,
		ChatJID:         chatJID,
		IsFromMe:        isFromMe,
		MediaType:       mediaType,
		Filename:        filename,
		QuotedMessageId: quotedMessageId,
		QuotedSender:    quotedSender,
		QuotedContent:   quotedContent,
	})
	if err != nil {
		fmt.Printf("CRITICAL: webhook observation was not queued: %v\n", err)
	}
}

// In main.go, handleMessage forwards webhooks for messages with text content.
// It will forward self-sent messages when the env var FORWARD_SELF=true.
