package main

// Group roster capture (OpenClaw incident 2026-10-13).
//
// A group message used to leave the bridge as {sender: "<15-digit LID>", chatJID: "<group>@g.us"}
// and nothing else: no group subject, no participant list, no phone or push name for the author.
// The bot therefore stored every group exchange with participants [] and an author nobody could
// resolve, and the brain advised on a group post without any way of knowing who was in the group.
//
// This file is capture only. It reads what WhatsApp already tells this device — the group's
// subject and participant list (GetGroupInfo), the LID→phone map and the contact push names the
// store already holds — and forwards them as observed facts. It decides nothing about them.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
)

// WebhookParticipant is one observed member of a group at roster time.
type WebhookParticipant struct {
	JID      string `json:"jid"`
	Phone    string `json:"phone,omitempty"`
	LID      string `json:"lid,omitempty"`
	PushName string `json:"pushName,omitempty"`
	IsAdmin  bool   `json:"isAdmin,omitempty"`
}

// GroupRoster is the observed membership of one group at one instant.
type GroupRoster struct {
	JID          string               `json:"jid"`
	Name         string               `json:"name,omitempty"`
	Participants []WebhookParticipant `json:"participants"`
	ObservedAt   time.Time            `json:"observedAt"`
}

// rosterCacheTTL bounds how often a group's roster is re-read from WhatsApp between messages.
// A GroupInfo event (join, leave, subject change) invalidates the entry immediately.
const rosterCacheTTL = time.Hour

var (
	rosterMu    sync.Mutex
	rosterCache = map[string]GroupRoster{}

	// fetchGroupInfo is the one network seam; tests replace it.
	fetchGroupInfo = func(ctx context.Context, client *whatsmeow.Client, jid types.JID) (*types.GroupInfo, error) {
		return client.GetGroupInfo(ctx, jid)
	}
)

func invalidateRoster(jid types.JID) {
	rosterMu.Lock()
	delete(rosterCache, jid.String())
	rosterMu.Unlock()
}

// phoneForJID returns the phone-number user part for a participant whose primary JID may be a
// LID. It uses, in order: an explicit phone JID, the phone the participant record carries, and
// the device's LID→phone map. An unresolvable LID yields "" rather than an invented number.
func phoneForJID(ctx context.Context, client *whatsmeow.Client, jid, phone types.JID) string {
	if jid.Server == types.DefaultUserServer && jid.User != "" {
		return jid.User
	}
	if phone.Server == types.DefaultUserServer && phone.User != "" {
		return phone.User
	}
	if jid.Server == types.HiddenUserServer && client != nil && client.Store != nil && client.Store.LIDs != nil {
		pn, err := client.Store.LIDs.GetPNForLID(ctx, jid)
		if err == nil && pn.Server == types.DefaultUserServer && pn.User != "" {
			return pn.User
		}
	}
	return ""
}

// pushNameFor returns the stored contact push/full name for a participant, or "".
func pushNameFor(ctx context.Context, client *whatsmeow.Client, candidates ...types.JID) string {
	if client == nil || client.Store == nil || client.Store.Contacts == nil {
		return ""
	}
	for _, jid := range candidates {
		if jid.IsEmpty() {
			continue
		}
		info, err := client.Store.Contacts.GetContact(ctx, jid)
		if err != nil || !info.Found {
			continue
		}
		for _, name := range []string{info.PushName, info.FullName, info.FirstName, info.BusinessName} {
			if strings.TrimSpace(name) != "" {
				return strings.TrimSpace(name)
			}
		}
	}
	return ""
}

func rosterFromGroupInfo(ctx context.Context, client *whatsmeow.Client, info *types.GroupInfo, observedAt time.Time) GroupRoster {
	roster := GroupRoster{JID: info.JID.String(), Name: strings.TrimSpace(info.GroupName.Name), ObservedAt: observedAt}
	for _, p := range info.Participants {
		lid := ""
		if p.LID.Server == types.HiddenUserServer {
			lid = p.LID.String()
		} else if p.JID.Server == types.HiddenUserServer {
			lid = p.JID.String()
		}
		phoneJID := p.PhoneNumber
		if phoneJID.IsEmpty() && p.JID.Server == types.DefaultUserServer {
			phoneJID = p.JID
		}
		phone := phoneForJID(ctx, client, p.JID, phoneJID)
		if phone != "" && phoneJID.IsEmpty() {
			phoneJID = types.JID{User: phone, Server: types.DefaultUserServer}
		}
		roster.Participants = append(roster.Participants, WebhookParticipant{
			JID:      p.JID.String(),
			Phone:    phone,
			LID:      lid,
			PushName: pushNameFor(ctx, client, phoneJID, p.JID, p.LID),
			IsAdmin:  p.IsAdmin || p.IsSuperAdmin,
		})
	}
	sort.Slice(roster.Participants, func(i, j int) bool { return roster.Participants[i].JID < roster.Participants[j].JID })
	return roster
}

// groupRoster returns the group's observed membership, from cache when fresh, otherwise from
// WhatsApp. A failed read returns the error; the caller forwards the message without a roster
// rather than with a stale or invented one.
func groupRoster(ctx context.Context, client *whatsmeow.Client, jid types.JID) (GroupRoster, error) {
	if jid.Server != types.GroupServer {
		return GroupRoster{}, fmt.Errorf("not a group jid: %s", jid)
	}
	key := jid.String()
	rosterMu.Lock()
	cached, ok := rosterCache[key]
	rosterMu.Unlock()
	if ok && time.Since(cached.ObservedAt) < rosterCacheTTL {
		return cached, nil
	}
	info, err := fetchGroupInfo(ctx, client, jid)
	if err != nil {
		return GroupRoster{}, err
	}
	if info == nil {
		return GroupRoster{}, fmt.Errorf("group info empty for %s", jid)
	}
	roster := rosterFromGroupInfo(ctx, client, info, time.Now().UTC())
	rosterMu.Lock()
	rosterCache[key] = roster
	rosterMu.Unlock()
	return roster, nil
}

// senderIdentity resolves who spoke: the raw JID as seen, the phone when WhatsApp or the LID
// map knows it, and the push name the message carried.
func senderIdentity(ctx context.Context, client *whatsmeow.Client, info types.MessageInfo) (jid, phone, pushName string) {
	jid = info.Sender.String()
	phone = phoneForJID(ctx, client, info.Sender, info.SenderAlt)
	if phone == "" && info.IsFromMe && client != nil && client.Store != nil && client.Store.ID != nil {
		phone = client.Store.ID.User
	}
	pushName = strings.TrimSpace(info.PushName)
	if pushName == "" {
		pushName = pushNameFor(ctx, client, info.SenderAlt, info.Sender)
	}
	return jid, phone, pushName
}
