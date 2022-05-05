package types

import (
	"time"
)

// Block struct to be stored in persistance layer
type Block struct {
	Height    int
	TxCount   int
	Producer  string
	Timestamp time.Time
}
