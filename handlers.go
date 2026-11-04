package matrixclient

import (
	"context"
	"slices"
	"strings"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

func (c *Client) channelLocked(roomID id.RoomID) *Channel {
	ch, ok := c.channels[roomID]
	if !ok {
		ch = &Channel{ID: roomID, Members: make(map[id.UserID]*User)}
		c.channels[roomID] = ch
	}

	return ch
}

// addDirectLocked marks roomID as direct with userID. A nil member keeps any
// cached profile of the user.
func (c *Client) addDirectLocked(roomID id.RoomID, userID id.UserID, member *event.MemberEventContent) {
	u, ok := c.users[userID]
	if !ok {
		u = &User{ID: userID}
		c.users[userID] = u
	}

	if member != nil {
		u.MemberEventContent = member
	}

	ch := c.channelLocked(roomID)

	ch.Lock()
	ch.IsDirect = true
	ch.Members[userID] = u
	ch.Unlock()

	if !slices.Contains(c.dmChannels[roomID], userID) {
		c.dmChannels[roomID] = append(c.dmChannels[roomID], userID)
	}
}

// handleDM rebuilds the direct room state, m.direct is a full snapshot.
func (c *Client) handleDM(_ context.Context, ev *event.Event) {
	chats := ev.Content.AsDirectChats()
	if chats == nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	for roomID := range c.dmChannels {
		if ch, ok := c.channels[roomID]; ok {
			ch.Lock()
			ch.IsDirect = false
			ch.Unlock()
		}
	}

	c.dmChannels = make(map[id.RoomID][]id.UserID)
	c.directData = make(map[id.UserID][]id.RoomID, len(*chats))

	for userID, rooms := range *chats {
		c.directData[userID] = slices.Clone(rooms)

		for _, roomID := range rooms {
			c.addDirectLocked(roomID, userID, nil)
		}
	}
}

func (c *Client) handleMember(_ context.Context, ev *event.Event) {
	member := ev.Content.AsMember()
	if member == nil || ev.StateKey == nil {
		return
	}

	targetID := id.UserID(*ev.StateKey)

	c.applyMember(ev, member, targetID)

	if h, ok := c.handler.(MemberHandler); ok && c.synced.Load() {
		h.OnMember(&MemberChange{
			RoomID:     ev.RoomID,
			UserID:     targetID,
			Sender:     ev.Sender,
			Membership: member.Membership,
			Reason:     member.Reason,
		})
	}
}

func (c *Client) applyMember(ev *event.Event, member *event.MemberEventContent, targetID id.UserID) {
	c.Lock()
	defer c.Unlock()

	u, ok := c.users[targetID]
	if !ok {
		u = &User{ID: targetID}
		c.users[targetID] = u
	}

	u.MemberEventContent = member

	ch := c.channelLocked(ev.RoomID)

	ch.Lock()
	switch member.Membership {
	case event.MembershipJoin, event.MembershipInvite:
		ch.Members[targetID] = u
	default:
		delete(ch.Members, targetID)
	}
	ch.Unlock()

	if member.IsDirect {
		c.addDirectLocked(ev.RoomID, targetID, member)
	}
}

func (c *Client) handleRoomName(_ context.Context, ev *event.Event) {
	name := ev.Content.AsRoomName()
	if name == nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	ch := c.channelLocked(ev.RoomID)

	ch.Lock()
	defer ch.Unlock()

	ch.RawName = name.Name
	ch.Name = ""
	if name.Name != "" {
		ch.Name = "#" + strings.ReplaceAll(name.Name, " ", "")
	}
}

func (c *Client) handleCanonicalAlias(_ context.Context, ev *event.Event) {
	alias := ev.Content.AsCanonicalAlias()
	if alias == nil {
		return
	}

	c.Lock()
	defer c.Unlock()

	ch := c.channelLocked(ev.RoomID)

	ch.Lock()
	ch.Alias, ch.AltAliases = alias.Alias, alias.AltAliases
	ch.Unlock()
}

// maxTrackedEvents bounds Channel.eventSeq, which keeps events that never decrypt.
const maxTrackedEvents = 1024

// noteTimeline records the sync timeline position of the messages in res before
// they are dispatched. Encrypted messages with a late room key are delivered
// out of order, so their position can only be taken from the sync response.
func (c *Client) noteTimeline(res *mautrix.RespSync) {
	c.Lock()
	defer c.Unlock()

	for roomID, room := range res.Rooms.Join {
		if room != nil {
			c.noteTimelineLocked(roomID, room.Timeline.Events)
		}
	}

	for roomID, room := range res.Rooms.Leave {
		if room != nil {
			c.noteTimelineLocked(roomID, room.Timeline.Events)
		}
	}
}

func (c *Client) noteTimelineLocked(roomID id.RoomID, events []*event.Event) {
	var ch *Channel

	for _, ev := range events {
		if ev.Type != event.EventMessage && ev.Type != event.EventEncrypted {
			continue
		}

		if ch == nil {
			ch = c.channelLocked(roomID)
			ch.Lock()

			if ch.eventSeq == nil {
				ch.eventSeq = make(map[id.EventID]uint64)
			}
		}

		ch.nextSeq++
		ch.eventSeq[ev.ID] = ch.nextSeq
	}

	if ch == nil {
		return
	}

	// Only events newer than lastSeq can still advance the marker. If even
	// those exceed the bound, the oldest go first as a newer one supersedes them.
	if len(ch.eventSeq) > maxTrackedEvents {
		pruneEventSeq(ch)
	}

	ch.Unlock()
}

// pruneEventSeq drops the positions that cannot advance LastEventID, then the
// oldest ones beyond maxTrackedEvents. The caller holds ch's lock.
func pruneEventSeq(ch *Channel) {
	for evID, seq := range ch.eventSeq {
		if seq <= ch.lastSeq {
			delete(ch.eventSeq, evID)
		}
	}

	if len(ch.eventSeq) <= maxTrackedEvents {
		return
	}

	seqs := make([]uint64, 0, len(ch.eventSeq))
	for _, seq := range ch.eventSeq {
		seqs = append(seqs, seq)
	}

	slices.Sort(seqs)

	cutoff := seqs[len(seqs)-maxTrackedEvents]
	for evID, seq := range ch.eventSeq {
		if seq < cutoff {
			delete(ch.eventSeq, evID)
		}
	}
}

// noteActivity records the newest message of a room, including
// those of the initial sync which are not delivered to the handler.
func (c *Client) noteActivity(roomID id.RoomID, eventID id.EventID, ts int64) {
	c.Lock()
	defer c.Unlock()

	ch := c.channelLocked(roomID)

	ch.Lock()
	ch.LastActivity = max(ch.LastActivity, ts)

	// The timeline position, not the arrival or decryption order, decides the
	// newest event, as origin_server_ts is not monotonic either.
	if seq, ok := ch.eventSeq[eventID]; ok {
		delete(ch.eventSeq, eventID)

		if seq > ch.lastSeq {
			ch.lastSeq, ch.LastEventID = seq, eventID
		}
	}

	ch.Unlock()
}

func (c *Client) handleMessage(_ context.Context, ev *event.Event) {
	content := ev.Content.AsMessage()
	if content == nil {
		return
	}

	c.noteActivity(ev.RoomID, ev.ID, ev.Timestamp)

	if targetID := content.RelatesTo.GetReplaceID(); targetID != "" {
		if h, ok := c.handler.(EditHandler); ok {
			if !c.synced.Load() {
				return
			}

			text := content.Body
			if content.NewContent != nil {
				text = content.NewContent.Body
			}

			h.OnEdit(&Edit{
				RoomID:    ev.RoomID,
				EventID:   ev.ID,
				TargetID:  targetID,
				Sender:    ev.Sender,
				Text:      text,
				Direct:    c.IsDirect(ev.RoomID),
				Timestamp: ev.Timestamp,
			})

			return
		}
	}

	if ev.Sender != c.mc.UserID {
		c.noteSender(ev.Sender, content.MsgType)
	}

	if !c.synced.Load() {
		return
	}

	msg := &Message{
		RoomID:    ev.RoomID,
		EventID:   ev.ID,
		Sender:    ev.Sender,
		Text:      content.Body,
		Direct:    c.IsDirect(ev.RoomID),
		Timestamp: ev.Timestamp,
		MsgType:   content.MsgType,
		URL:       content.URL,
		FileName:  content.GetFileName(),
		Info:      content.Info,
		File:      content.File,
	}
	if content.RelatesTo != nil {
		msg.ParentID = content.RelatesTo.EventID
	}

	c.handler.OnMessage(msg)
}

func (c *Client) handleReaction(_ context.Context, ev *event.Event) {
	if !c.synced.Load() {
		return
	}

	content := ev.Content.AsReaction()
	if content == nil || content.RelatesTo.EventID == "" {
		return
	}

	c.handler.OnReaction(&Reaction{
		RoomID:   ev.RoomID,
		ParentID: content.RelatesTo.EventID,
		Sender:   ev.Sender,
		Key:      content.RelatesTo.Key,
		Direct:   c.IsDirect(ev.RoomID),
	})
}

func (c *Client) handleRedaction(_ context.Context, ev *event.Event) {
	h, ok := c.handler.(RedactionHandler)
	if !ok || !c.synced.Load() {
		return
	}

	targetID := ev.Redacts
	if content := ev.Content.AsRedaction(); content != nil && content.Redacts != "" {
		targetID = content.Redacts
	}

	if targetID == "" {
		return
	}

	h.OnRedaction(&Redaction{
		RoomID:   ev.RoomID,
		TargetID: targetID,
		Sender:   ev.Sender,
		Direct:   c.IsDirect(ev.RoomID),
	})
}

func (c *Client) handleTyping(_ context.Context, ev *event.Event) {
	h, ok := c.handler.(TypingHandler)
	if !ok || !c.synced.Load() {
		return
	}

	content := ev.Content.AsTyping()
	if content == nil {
		return
	}

	users := make([]id.UserID, 0, len(content.UserIDs))

	for _, userID := range content.UserIDs {
		if userID != c.mc.UserID {
			users = append(users, userID)
		}
	}

	h.OnTyping(&Typing{RoomID: ev.RoomID, Users: users, Direct: c.IsDirect(ev.RoomID)})
}
