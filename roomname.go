package matrixclient

import (
	"context"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/id"
)

const (
	maxFallbackNames = 3
	shortIDLength    = 8
)

// nameSanitizer drops what is not valid in an IRC channel name.
var nameSanitizer = strings.NewReplacer(" ", "", ",", "", ":", "")

// displayNameLocked returns the channel name of a room: its canonical alias, its
// name, the members as Element names rooms without a name, or a short room ID.
// c must be read locked and ch locked.
func (c *Client) displayNameLocked(roomID id.RoomID, ch *Channel) string {
	switch {
	case ch.Alias != "":
		return c.roomName(ch.Alias)
	case ch.Name != "":
		return strings.Replace(ch.Name, ":", "/", 1)
	}

	name := c.memberNameLocked(ch)
	if name != "" {
		return name
	}

	return "#unnamed-" + shortRoomID(roomID)
}

// memberNameLocked names a room after its other members, preferring the heroes the
// server picked for it. It returns "" if there are none. c must be read locked
// and ch locked.
func (c *Client) memberNameLocked(ch *Channel) string {
	self := c.mc.UserID

	others := len(ch.Members)
	if _, ok := ch.Members[self]; ok {
		others--
	}

	var picked [maxFallbackNames]id.UserID

	n := 0

	if len(ch.heroes) > 0 {
		for _, userID := range ch.heroes {
			if userID != self && n < len(picked) {
				picked[n] = userID
				n++
			}
		}
	} else {
		// The smallest IDs, so the name does not depend on the map order.
		for userID := range ch.Members {
			if userID != self {
				n = insertSmallest(&picked, n, userID)
			}
		}
	}

	if n == 0 {
		return ""
	}

	var b strings.Builder

	b.WriteByte('#')

	for i, userID := range picked[:n] {
		if i > 0 {
			b.WriteByte('-')
		}

		b.WriteString(c.userLabelLocked(userID))
	}

	if others > n {
		b.WriteByte('+')
		b.WriteString(strconv.Itoa(others - n))
	}

	return b.String()
}

// insertSmallest keeps the n smallest IDs seen in picked, sorted, and returns the new n.
func insertSmallest(picked *[maxFallbackNames]id.UserID, n int, userID id.UserID) int {
	i := n

	if n == len(picked) {
		if userID >= picked[n-1] {
			return n
		}

		i = n - 1
	} else {
		n++
	}

	for ; i > 0 && picked[i-1] > userID; i-- {
		picked[i] = picked[i-1]
	}

	picked[i] = userID

	return n
}

// userLabelLocked is the display name of a user, or the localpart of its ID. c must be read locked.
func (c *Client) userLabelLocked(userID id.UserID) string {
	if u, ok := c.users[userID]; ok && u.MemberEventContent != nil {
		if label := nameSanitizer.Replace(u.Displayname); label != "" {
			return label
		}
	}

	local, _, _ := strings.Cut(strings.TrimPrefix(string(userID), "@"), ":")

	return nameSanitizer.Replace(local)
}

func shortRoomID(roomID id.RoomID) string {
	short, _, _ := strings.Cut(strings.TrimPrefix(string(roomID), "!"), ":")

	return short[:min(len(short), shortIDLength)]
}

// ResolveRoomName returns the channel name of roomID, as Rooms lists it. A room
// known only by its ID is first looked up on the server, bounded by ctx, so a
// caller can pass a short deadline before creating the channel. It returns ""
// for an unknown room.
func (c *Client) ResolveRoomName(ctx context.Context, roomID id.RoomID) string {
	c.RLock()
	ch, ok := c.channels[roomID]
	c.RUnlock()

	if !ok {
		return ""
	}

	ch.RLock()
	unnamed := ch.Name == "" && ch.Alias == "" && !ch.nameChecked
	ch.RUnlock()

	if unnamed {
		err := c.RefreshRoomName(ctx, roomID)
		if err != nil {
			c.logger.Debugf("resolving name of %s: %s", roomID, err)
		}
	}

	c.RLock()
	defer c.RUnlock()

	ch.Lock()
	defer ch.Unlock()

	ch.reportedName = c.displayNameLocked(roomID, ch)

	return ch.reportedName
}

// noteRoomName delivers a RoomRename if the channel name of roomID differs from
// the one last reported through Rooms, ResolveRoomName or a RoomRename.
func (c *Client) noteRoomName(roomID id.RoomID) {
	h, ok := c.handler.(RoomNameHandler)
	if !ok || !c.synced.Load() {
		return
	}

	c.RLock()
	ch, ok := c.channels[roomID]

	if !ok {
		c.RUnlock()

		return
	}

	ch.Lock()
	old := ch.reportedName

	var name string

	if old != "" && !ch.IsDirect {
		name = c.displayNameLocked(roomID, ch)
		if name != old {
			ch.reportedName = name
		}
	}

	ch.Unlock()
	c.RUnlock()

	if name != "" && name != old {
		h.OnRoomName(&RoomRename{RoomID: roomID, OldName: old, Name: name})
	}
}
