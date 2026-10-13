package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

var (
	golemGroup = types.JID{User: "120363410421473533", Server: types.GroupServer}
	meirLID    = types.JID{User: "185869189292186", Server: types.HiddenUserServer}
	meirPN     = types.JID{User: "972544401224", Server: types.DefaultUserServer}
	otherLID   = types.JID{User: "274573014597664", Server: types.HiddenUserServer}
	paulLID    = types.JID{User: "59463973241000", Server: types.HiddenUserServer}
	paulPN     = types.JID{User: "971509505525", Server: types.DefaultUserServer}
)

func withFetch(t *testing.T, fn func(context.Context, *whatsmeow.Client, types.JID) (*types.GroupInfo, error)) {
	t.Helper()
	previous := fetchGroupInfo
	fetchGroupInfo = fn
	rosterMu.Lock()
	rosterCache = map[string]GroupRoster{}
	rosterMu.Unlock()
	t.Cleanup(func() { fetchGroupInfo = previous })
}

func golemInfo() *types.GroupInfo {
	info := &types.GroupInfo{JID: golemGroup}
	info.GroupName.Name = "PG & Golem"
	info.Participants = []types.GroupParticipant{
		{JID: meirLID, LID: meirLID, IsAdmin: true},       // phone only via the LID map
		{JID: otherLID, LID: otherLID},                    // unresolvable: no phone invented
		{JID: paulLID, LID: paulLID, PhoneNumber: paulPN}, // phone carried by WhatsApp
	}
	return info
}

func TestRosterFromGroupInfo_ResolvesPhonesWithoutInventingThem(t *testing.T) {
	client := newTestClient(&mockLIDStore{pnByLID: map[types.JID]types.JID{meirLID: meirPN}})
	roster := rosterFromGroupInfo(context.Background(), client, golemInfo(), time.Date(2026, 10, 13, 3, 0, 0, 0, time.UTC))

	if roster.Name != "PG & Golem" || roster.JID != golemGroup.String() {
		t.Fatalf("roster identity wrong: %+v", roster)
	}
	byJID := map[string]WebhookParticipant{}
	for _, p := range roster.Participants {
		byJID[p.JID] = p
	}
	if got := byJID[meirLID.String()]; got.Phone != "972544401224" || got.LID != meirLID.String() || !got.IsAdmin {
		t.Errorf("LID-map resolution failed: %+v", got)
	}
	if got := byJID[otherLID.String()]; got.Phone != "" || got.LID != otherLID.String() {
		t.Errorf("unresolvable LID must carry no phone: %+v", got)
	}
	if got := byJID[paulLID.String()]; got.Phone != "971509505525" {
		t.Errorf("carried phone lost: %+v", got)
	}
}

func TestGroupRoster_CachesUntilInvalidated(t *testing.T) {
	calls := 0
	withFetch(t, func(context.Context, *whatsmeow.Client, types.JID) (*types.GroupInfo, error) {
		calls++
		return golemInfo(), nil
	})
	client := newTestClient(&mockLIDStore{})
	if _, err := groupRoster(context.Background(), client, golemGroup); err != nil {
		t.Fatal(err)
	}
	if _, err := groupRoster(context.Background(), client, golemGroup); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("expected one network read while cached, got %d", calls)
	}
	invalidateRoster(golemGroup)
	if _, err := groupRoster(context.Background(), client, golemGroup); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("expected a re-read after invalidation, got %d", calls)
	}
}

func TestGroupRoster_RefusesNonGroup(t *testing.T) {
	withFetch(t, func(context.Context, *whatsmeow.Client, types.JID) (*types.GroupInfo, error) {
		t.Fatal("must not fetch for a person jid")
		return nil, nil
	})
	if _, err := groupRoster(context.Background(), newTestClient(&mockLIDStore{}), meirPN); err == nil {
		t.Fatal("expected an error for a non-group jid")
	}
}

func TestDescribeObservation_GroupMessageCarriesRosterAndSender(t *testing.T) {
	withFetch(t, func(context.Context, *whatsmeow.Client, types.JID) (*types.GroupInfo, error) {
		return golemInfo(), nil
	})
	client := newTestClient(&mockLIDStore{pnByLID: map[types.JID]types.JID{meirLID: meirPN}})
	msg := buildTextMessage(golemGroup, meirLID, meirPN, types.EmptyJID, false, "The E was confusing")
	msg.Info.PushName = "Meir"

	payload := describeObservation(client, msg, golemGroup, "", meirLID.User, "The E was confusing", golemGroup.String(), "", "", "", "", "", testLogger())

	if !payload.IsGroup || payload.ChatName != "PG & Golem" {
		t.Errorf("group identity missing: %+v", payload)
	}
	if payload.SenderJID != meirLID.String() || payload.SenderPhone != "972544401224" || payload.SenderPushName != "Meir" {
		t.Errorf("sender identity missing: jid=%s phone=%s push=%s", payload.SenderJID, payload.SenderPhone, payload.SenderPushName)
	}
	if len(payload.Participants) != 3 || payload.RosterObservedAt == "" {
		t.Errorf("roster missing: %+v", payload)
	}
	// WhatsApp's own send time (ef4e3d1) must survive the richer payload.
	if payload.Timestamp == "" {
		t.Errorf("provider timestamp lost: %+v", payload)
	}
	// Legacy fields are untouched so the bot's existing capture keeps working unchanged.
	if payload.Sender != meirLID.User || payload.ChatJID != golemGroup.String() {
		t.Errorf("legacy fields changed: %+v", payload)
	}
	body, _ := json.Marshal(payload)
	for _, key := range []string{`"chatName"`, `"senderJid"`, `"senderPhone"`, `"participants"`, `"rosterObservedAt"`} {
		if !contains(string(body), key) {
			t.Errorf("json lacks %s: %s", key, body)
		}
	}
}

func TestDescribeObservation_RosterFailureForwardsWithoutRoster(t *testing.T) {
	withFetch(t, func(context.Context, *whatsmeow.Client, types.JID) (*types.GroupInfo, error) {
		return nil, errors.New("not connected")
	})
	client := newTestClient(&mockLIDStore{})
	msg := buildTextMessage(golemGroup, otherLID, types.EmptyJID, types.EmptyJID, false, "hello")

	payload := describeObservation(client, msg, golemGroup, "PG & Golem", otherLID.User, "hello", golemGroup.String(), "", "", "", "", "", testLogger())

	if payload.Participants != nil || payload.RosterObservedAt != "" {
		t.Errorf("a failed roster read must not fabricate membership: %+v", payload)
	}
	if payload.SenderPhone != "" {
		t.Errorf("an unresolvable LID must not become a phone: %+v", payload)
	}
	if payload.ChatName != "PG & Golem" || !payload.IsGroup {
		t.Errorf("chat identity lost: %+v", payload)
	}
}

func TestDescribeObservation_DirectMessageHasNoRoster(t *testing.T) {
	withFetch(t, func(context.Context, *whatsmeow.Client, types.JID) (*types.GroupInfo, error) {
		t.Fatal("direct messages never read a roster")
		return nil, nil
	})
	client := newTestClient(&mockLIDStore{})
	msg := buildTextMessage(meirPN, meirPN, types.EmptyJID, types.EmptyJID, false, "hi")
	payload := describeObservation(client, msg, meirPN, "Meir", meirPN.User, "hi", meirPN.String(), "", "", "", "", "", testLogger())
	if payload.IsGroup || payload.Participants != nil || payload.SenderPhone != "972544401224" {
		t.Errorf("direct message shape wrong: %+v", payload)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
