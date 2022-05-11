package postgresdriver

import (
	"database/sql/driver"
	"encoding/json"
	"errors"

	"github.com/pokt-foundation/pocket-go/provider"
)

var (
	// ErrByteTypeAssertionFailed error when []byte type assertion fails
	ErrByteTypeAssertionFailed = errors.New("type assertion to []byte failed")
)

// txResult is a wrapper for provider.TxResult to implement interfaces for JSONB parsing
type txResult struct {
	*provider.TxResult
}

// Make the txResult struct implement the driver.Valuer interface. This method
// simply returns the JSON-encoded representation of the struct.
func (r *txResult) Value() (driver.Value, error) {
	return json.Marshal(r)
}

// Make the txResult struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (r *txResult) Scan(value any) error {
	b, ok := value.([]byte)
	if !ok {
		return ErrByteTypeAssertionFailed
	}

	return json.Unmarshal(b, &r)
}

// proof is a wrapper for provider.TransactionProof to implement interfaces for JSONB parsing
type proof struct {
	*provider.TransactionProof
}

// Make the proof struct implement the driver.Valuer interface. This method
// simply returns the JSON-encoded representation of the struct.
func (p *proof) Value() (driver.Value, error) {
	return json.Marshal(p)
}

// Make the proof struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (p *proof) Scan(value any) error {
	b, ok := value.([]byte)
	if !ok {
		return ErrByteTypeAssertionFailed
	}

	return json.Unmarshal(b, &p)
}

// stdTx is a wrapper for provider.StdTx to implement interfaces for JSONB parsing
type stdTx struct {
	*provider.StdTx
}

// Make the stdTx struct implement the driver.Valuer interface. This method
// simply returns the JSON-encoded representation of the struct.
func (s *stdTx) Value() (driver.Value, error) {
	return json.Marshal(s)
}

// Make the stdTx struct implement the sql.Scanner interface. This method
// simply decodes a JSON-encoded value into the struct fields.
func (s *stdTx) Scan(value any) error {
	b, ok := value.([]byte)
	if !ok {
		return ErrByteTypeAssertionFailed
	}

	return json.Unmarshal(b, &s)
}
