package types

import (
	"math/big"

	"github.com/pokt-foundation/pocket-go/provider"
)

// Transaction struct handler of all transaction fields to be indexed
type Transaction struct {
	Hash            string
	FromAddress     string
	ToAddress       string
	AppPubKey       string
	Blockchains     []string
	MessageType     string
	Height          int
	Index           int
	StdTx           *provider.StdTx
	TxResult        *provider.TxResult
	Tx              string
	Entropy         int
	Fee             int
	FeeDenomination string
	Amount          *big.Int
}

// ReadTransactionsOptions optional parameters for ReadTransactions
type ReadTransactionsOptions struct {
	PerPage int
	Page    int
	Order   Order
}

// ReadTransactionsByAddressOptions optional parameters for ReadTransactionsByAddress
type ReadTransactionsByAddressOptions struct {
	PerPage int
	Page    int
}

// ReadTransactionsByHeightOptions optional parameters for ReadTransactionsByHeight
type ReadTransactionsByHeightOptions struct {
	PerPage int
	Page    int
}
