package types

import (
	"math/big"
)

type Transaction struct {
	hash     string
	sender   string
	receiver string
	amount   big.Int
	fee      big.Int
	height   int
	code     int
	status   string
	memo     string
}
