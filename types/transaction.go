package types

import (
	"math/big"
)

type Transaction struct {
	Hash     string
	Sender   string
	Receiver string
	Amount   big.Int
	Fee      big.Int
	Height   int
	Code     int
	Status   string
	Memo     string
}
