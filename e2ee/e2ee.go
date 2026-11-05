// Package e2ee enables end-to-end encrypted rooms for matrixclient.
//
// Build with the goolm tag (pure Go olm) or have libolm installed, and with
// cgo enabled for the sqlite database holding the keys:
//
//	go build -tags goolm
package e2ee

import (
	"context"
	"fmt"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/crypto/cryptohelper"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"github.com/matterbridge/matrixclient"
)

// Config configures encryption.
type Config struct {
	// DBPath is the sqlite database storing the device keys and sessions. Keep
	// it: losing it makes the device unable to decrypt earlier messages.
	DBPath string
	// PickleKey encrypts the keys in the database and must stay the same.
	PickleKey []byte
	// OnDecryptError is called for messages that could not be decrypted. Optional.
	OnDecryptError func(ev *event.Event, err error)
}

// helper reports terminal decryption failures to the client, so it can drop
// the ordering data kept for the undelivered message.
type helper struct {
	*cryptohelper.CryptoHelper

	client *mautrix.Client
}

func (h helper) OnDecryptFailure(f func(ev *event.Event, err error)) {
	prev := h.DecryptErrorCallback

	h.DecryptErrorCallback = func(ev *event.Event, err error) {
		f(ev, err)

		if prev != nil {
			prev(ev, err)
		}
	}
}

// Fingerprint implements matrixclient.FingerprintProvider.
func (h helper) Fingerprint() string {
	m := h.Machine()
	if m == nil {
		return ""
	}

	return m.Fingerprint()
}

// DeviceVerified implements matrixclient.DeviceVerifier.
func (h helper) DeviceVerified(ctx context.Context) (bool, error) {
	m := h.Machine()
	if m == nil {
		return false, nil
	}

	var (
		dev *id.Device
		err error
	)

	// GetOrFetchDevice would return the cached device, whose trust may predate a
	// verification done from another client, so refresh from the server. FetchKeys
	// reaches into StateStore, which is not set up until the machine is fully
	// initialised, so use the read-only store until then.
	if m.StateStore == nil {
		dev, err = m.CryptoStore.GetDevice(ctx, h.client.UserID, h.client.DeviceID)
		if err != nil {
			return false, fmt.Errorf("get own device: %w", err)
		}
	} else {
		var devices map[id.UserID]map[id.DeviceID]*id.Device

		devices, err = m.FetchKeys(ctx, []id.UserID{h.client.UserID}, true)
		if err != nil {
			return false, fmt.Errorf("fetch own device keys: %w", err)
		}

		dev = devices[h.client.UserID][h.client.DeviceID]
	}

	if dev == nil {
		return false, nil
	}

	return m.IsDeviceTrusted(ctx, dev), nil
}

// Factory returns the option to pass to matrixclient.New.
func Factory(cfg Config) matrixclient.Option {
	return matrixclient.WithCrypto(func(mc *mautrix.Client, login *mautrix.ReqLogin) (matrixclient.Crypto, error) {
		ch, err := cryptohelper.NewCryptoHelper(mc, cfg.PickleKey, cfg.DBPath)
		if err != nil {
			return nil, err
		}

		if login != nil {
			ch.LoginAs = login
		}

		if cfg.OnDecryptError != nil {
			ch.DecryptErrorCallback = cfg.OnDecryptError
		}

		return helper{CryptoHelper: ch, client: mc}, nil
	})
}
