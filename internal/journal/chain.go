package journal

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Genesis is the predecessor digest of the first retained record. A consumer
// seeded at history base 0 must see this exact value before applying anything.
const Genesis = "0000000000000000000000000000000000000000000000000000000000000000"

// Stamp binds a record to its predecessor and computes its own digest. The
// digest covers every field except Digest itself, so Prev and Digest together
// form a hash chain over the complete sequence of records.
func Stamp(r Record) (Record, error) {
	r.Digest = ""
	payload, err := json.Marshal(r)
	if err != nil {
		return r, err
	}
	sum := sha256.Sum256(payload)
	r.Digest = hex.EncodeToString(sum[:])
	return r, nil
}

// Verify checks that the record still hashes to its recorded digest.
func Verify(r Record) error {
	if r.Digest == "" {
		return fmt.Errorf("record %d has no lineage digest", r.Sequence)
	}
	if len(r.Prev) != len(Genesis) {
		return fmt.Errorf("record %d has malformed predecessor", r.Sequence)
	}
	stamped, err := Stamp(r)
	if err != nil {
		return err
	}
	if stamped.Digest != r.Digest {
		return fmt.Errorf("record %d digest mismatch", r.Sequence)
	}
	return nil
}

// ChainHead replays the lineage chain over records and returns the digest at
// sequence at, or Genesis when no record at or below at is present. It is used
// when a checkpoint has moved past an archive that must be rebuilt from the
// journal.
func ChainHead(records []Record, at uint64) (string, error) {
	chain := Genesis
	for _, r := range records {
		if r.Sequence > at {
			break
		}
		copy := r
		copy.Prev = chain
		copy.Digest = ""
		stamped, err := Stamp(copy)
		if err != nil {
			return "", err
		}
		chain = stamped.Digest
	}
	return chain, nil
}
