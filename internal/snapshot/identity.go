package snapshot

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

// Identity names one logical source and one incarnation of its history.
//
// SourceID is stable while a database is rebuilt from its own backups; Epoch
// changes on every restore. A consumer that only compared sequence numbers
// could otherwise continue past a restore whose history was rebuilt from an
// unrelated point. Origin records the source identity and sequence a restore
// was derived from, so a consumer can report why it had to reinitialize.
const (
	IdentityFile = "identity.json"
	MaxIdentity  = 4 << 10
)

var tokenPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

type Identity struct {
	Format   int     `json:"format"`
	SourceID string  `json:"source_id"`
	Epoch    string  `json:"epoch"`
	Origin   *Origin `json:"origin,omitempty"`
}

func NewToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func NewIdentity() (Identity, error) {
	source, err := NewToken()
	if err != nil {
		return Identity{}, err
	}
	epoch, err := NewToken()
	if err != nil {
		return Identity{}, err
	}
	return Identity{Format: 1, SourceID: source, Epoch: epoch}, nil
}

// InheritIdentity creates the identity of a database restored from image. The
// logical source is preserved, the incarnation is new, and origin names the
// exact backup lineage the restored state came from.
func InheritIdentity(sourceID string, sequence uint64) (Identity, error) {
	if sourceID == "" {
		return NewIdentity()
	}
	if !tokenPattern.MatchString(sourceID) {
		return Identity{}, fmt.Errorf("invalid backup source identity")
	}
	epoch, err := NewToken()
	if err != nil {
		return Identity{}, err
	}
	return Identity{Format: 1, SourceID: sourceID, Epoch: epoch, Origin: &Origin{SourceID: sourceID, Sequence: sequence}}, nil
}

func (i Identity) Validate() error {
	if i.Format != 1 || !tokenPattern.MatchString(i.SourceID) || !tokenPattern.MatchString(i.Epoch) {
		return fmt.Errorf("invalid database identity")
	}
	if i.Origin != nil {
		if !tokenPattern.MatchString(i.Origin.SourceID) {
			return fmt.Errorf("invalid origin identity")
		}
	}
	return nil
}

func WriteIdentity(dir string, identity Identity) error {
	if err := identity.Validate(); err != nil {
		return err
	}
	b, err := json.Marshal(identity)
	if err != nil {
		return err
	}
	return Atomic(filepath.Join(dir, IdentityFile), b)
}

// LoadIdentity reads the directory identity. A database checkpointed before
// identity tracking migrates once: the supplied fallback, when complete, is
// adopted verbatim so the existing lineage keeps the same source and epoch.
// The boolean reports whether a new identity file was written.
func LoadIdentity(dir string, fallback *Identity) (Identity, bool, error) {
	path := filepath.Join(dir, IdentityFile)
	b, err := os.ReadFile(path)
	if err == nil {
		if len(b) > MaxIdentity {
			return Identity{}, false, fmt.Errorf("identity exceeds limit")
		}
		var identity Identity
		if err = json.Unmarshal(b, &identity); err != nil {
			return Identity{}, false, fmt.Errorf("identity: %w", err)
		}
		if err = identity.Validate(); err != nil {
			return Identity{}, false, err
		}
		return identity, false, nil
	}
	if !os.IsNotExist(err) {
		return Identity{}, false, err
	}
	var identity Identity
	if fallback != nil {
		identity = *fallback
		identity.Format = 1
		if identity.Validate() != nil {
			identity, err = NewIdentity()
		}
	} else {
		identity, err = NewIdentity()
	}
	if err != nil {
		return Identity{}, false, err
	}
	if err = WriteIdentity(dir, identity); err != nil {
		return Identity{}, false, err
	}
	return identity, true, nil
}
