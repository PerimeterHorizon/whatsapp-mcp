package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestWebhookOutboxPersistsAndRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		mac := hmac.New(sha256.New, []byte("test-secret"))
		_, _ = mac.Write(body)
		wantSignature := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if got := r.Header.Get("X-OpenClaw-Signature"); got != wantSignature {
			t.Fatalf("signature = %q, want %q", got, wantSignature)
		}
		if calls.Add(1) == 1 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	outbox, err := openWebhookOutbox(filepath.Join(t.TempDir(), "outbox.db"), server.URL, "test-secret")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.db.Close()

	payload := WebhookPayload{MessageID: "message-1", ChatJID: "chat-1", Sender: "sender", Content: "evidence"}
	if err := outbox.enqueue(payload); err != nil {
		t.Fatal(err)
	}
	if delivered, err := outbox.deliverOne(); err != nil || !delivered {
		t.Fatalf("first delivery: delivered=%v err=%v", delivered, err)
	}

	if _, err := outbox.db.Exec("UPDATE webhook_outbox SET next_attempt_at = 0"); err != nil {
		t.Fatal(err)
	}
	if delivered, err := outbox.deliverOne(); err != nil || !delivered {
		t.Fatalf("second delivery: delivered=%v err=%v", delivered, err)
	}

	var status string
	var attempts int
	if err := outbox.db.QueryRow("SELECT status, attempts FROM webhook_outbox WHERE message_id = ?", "message-1").Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "delivered" || attempts != 1 {
		t.Fatalf("status=%q attempts=%d, want delivered/1", status, attempts)
	}
}

func TestWebhookOutboxDoesNotRequeueDeliveredObservation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	outbox, err := openWebhookOutbox(filepath.Join(t.TempDir(), "outbox.db"), server.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	defer outbox.db.Close()

	payload := WebhookPayload{MessageID: "message-1", ChatJID: "chat-1", Content: "first"}
	if err := outbox.enqueue(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.deliverOne(); err != nil {
		t.Fatal(err)
	}
	payload.Content = "duplicate"
	if err := outbox.enqueue(payload); err != nil {
		t.Fatal(err)
	}
	if delivered, err := outbox.deliverOne(); err != nil || delivered {
		t.Fatalf("duplicate delivery: delivered=%v err=%v", delivered, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls=%d, want 1", got)
	}
}
