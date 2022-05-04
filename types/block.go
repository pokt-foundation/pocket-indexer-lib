package types

import (
	"time"
)

type Block struct {
	height     int
	txCount    int
	relayCount int
	producer   string
	timestamp  time.Time
}
