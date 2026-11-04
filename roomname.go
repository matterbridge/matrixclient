package matrixclient

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

const (
	maxFallbackNames = 3
	shortIDLength    = 8

	// Same retry policy as matterclient's channel name healing.
	nameHealInitialDelay = 5 * time.Second
	nameHealMaxDelay     = 5 * time.Minute
	nameHealMaxAttempts  = 12

	nameLookupTimeout = 30 * time.Second
	maxNameLookups    = 5
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

// nameUnresolvedLocked reports whether the room has neither a name nor an alias and
// the server was not yet confirmed to have none, so its name may still be missing
// because the state was lost. ch must be locked.
func (ch *Channel) nameUnresolvedLocked() bool {
	return ch.Name == "" && ch.Alias == "" && !ch.IsDirect && !ch.nameChecked
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
	unresolved := ch.nameUnresolvedLocked()
	ch.RUnlock()

	if unresolved {
		err := c.RefreshRoomName(ctx, roomID)
		if err != nil {
			c.logger.Debugf("resolving name of %s: %s", roomID, err)
			c.healRoomName(roomID)
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

	var name, display string

	if old != "" && !ch.IsDirect {
		name = c.displayNameLocked(roomID, ch)
		if name != old {
			ch.reportedName = name
			display = ch.RawName
		}
	}

	ch.Unlock()
	c.RUnlock()

	if name != "" && name != old {
		h.OnRoomName(&RoomRename{RoomID: roomID, OldName: old, Name: name, DisplayName: display})
	}
}

// noteRoomNames reports the channel name changes of the rooms in res, once all
// its events were applied so a burst of state events yields a single change, and
// heals the rooms that are still without a name.
func (c *Client) noteRoomNames(res *mautrix.RespSync) {
	_, notify := c.handler.(RoomNameHandler)

	for roomID := range res.Rooms.Join {
		if notify {
			c.noteRoomName(roomID)
		}

		c.RLock()
		ch, ok := c.channels[roomID]
		c.RUnlock()

		if !ok {
			continue
		}

		ch.RLock()
		unresolved := ch.nameUnresolvedLocked()
		ch.RUnlock()

		if unresolved {
			c.healRoomName(roomID)
		}
	}
}

// healRoomName starts, at most once per room, a background retry with exponential
// backoff that refetches the name and alias of a room that has neither, for
// example because the state was lost during a failed sync. It stops on success,
// when the room is gone, after nameHealMaxAttempts, or when the client stops.
// OnRoomNameResolved is called once the name changed from the one reported.
func (c *Client) healRoomName(roomID id.RoomID) {
	if _, running := c.nameHealing.LoadOrStore(roomID, struct{}{}); running {
		return
	}

	go func() {
		defer c.nameHealing.Delete(roomID)

		delay := nameHealInitialDelay

		for attempt := 1; attempt <= nameHealMaxAttempts; attempt++ {
			resolved, final := c.attemptRoomName(roomID)
			if final {
				return
			}

			if resolved {
				c.logger.Infof("room name for %s resolved after %d attempt(s)", roomID, attempt)
				c.reportResolvedName(roomID)

				return
			}

			c.logger.Debugf("room name for %s still unresolved (attempt %d/%d), retrying in %s",
				roomID, attempt, nameHealMaxAttempts, delay)

			select {
			case <-c.ctx.Done():
				return
			case <-time.After(delay):
			}

			delay = min(delay*2, nameHealMaxDelay)
		}

		c.logger.Warnf("giving up resolving room name for %s", roomID)
	}()
}

// attemptRoomName makes one lookup. It returns whether the room now has a name or
// alias, and whether retrying is pointless: the room is gone, the server confirmed
// it has neither (like a 404 on a Mattermost channel), or the client stopped.
func (c *Client) attemptRoomName(roomID id.RoomID) (bool, bool) {
	select {
	case <-c.ctx.Done():
		return false, true
	case c.nameLookups <- struct{}{}:
	}

	err := c.RefreshRoomName(c.ctx, roomID)

	<-c.nameLookups

	if c.ctx.Err() != nil || errors.Is(err, mautrix.MForbidden) || errors.Is(err, mautrix.MUnknownToken) {
		return false, true
	}

	c.RLock()
	ch, ok := c.channels[roomID]
	c.RUnlock()

	if !ok {
		return false, true
	}

	ch.RLock()
	defer ch.RUnlock()

	return ch.Name != "" || ch.Alias != "", ch.nameChecked
}

// reportResolvedName calls OnRoomNameResolved with the healed name, unless a rename
// from the sync already reported it.
func (c *Client) reportResolvedName(roomID id.RoomID) {
	cb := c.OnRoomNameResolved
	if cb == nil {
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
	name := c.displayNameLocked(roomID, ch)

	changed := name != old
	if changed {
		ch.reportedName = name
	}

	ch.Unlock()
	c.RUnlock()

	if changed {
		cb(roomID, name)
	}
}

// RefreshRoomName fetches the name and canonical alias of roomID from the server
// and applies the ones the room does not have yet. A room the server reports as
// having neither is marked as such and not healed again, later changes arrive
// through the sync. Unlike ResolveRoomName it does not report anything.
func (c *Client) RefreshRoomName(ctx context.Context, roomID id.RoomID) error {
	ctx, cancel := context.WithTimeout(ctx, nameLookupTimeout)
	defer cancel()

	var (
		name  event.RoomNameEventContent
		alias event.CanonicalAliasEventContent
	)

	nameErr := c.mc.StateEvent(ctx, roomID, event.StateRoomName, "", &name)
	aliasErr := c.mc.StateEvent(ctx, roomID, event.StateCanonicalAlias, "", &alias)

	missing := errors.Is(nameErr, mautrix.MNotFound) && errors.Is(aliasErr, mautrix.MNotFound)

	var err error
	if nameErr != nil && !errors.Is(nameErr, mautrix.MNotFound) {
		err = nameErr
	}

	if aliasErr != nil && !errors.Is(aliasErr, mautrix.MNotFound) {
		err = errors.Join(err, aliasErr)
	}

	c.RLock()
	ch, ok := c.channels[roomID]
	c.RUnlock()

	if !ok {
		return err
	}

	ch.Lock()
	defer ch.Unlock()

	// A sync event that arrived in the meantime is newer than the fetched state.
	if ch.Name == "" && name.Name != "" {
		ch.setNameLocked(name.Name)
	}

	if ch.Alias == "" && alias.Alias != "" {
		ch.Alias, ch.AltAliases = alias.Alias, alias.AltAliases
	}

	ch.nameChecked = missing

	return err
}
