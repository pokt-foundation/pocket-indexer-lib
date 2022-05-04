package types

import (
	"time"
)

type Block struct {
	Height    int
	TxCount   int
	Producer  string
	Timestamp time.Time
}
