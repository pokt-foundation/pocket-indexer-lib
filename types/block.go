package types

import "time"

// Block struct handler of all block fields to be indexed
type Block struct {
	Hash             string
	Height           int
	Time             time.Time
	ProposerAddress  string
	TXCount          int
	TXTotal          int
	AccountsQuantity int
	AppsQuantity     int
	NodesQuantity    int
	Took             time.Duration
}

// ReadBlocksOptions optional parameters for ReadBlocks
type ReadBlocksOptions struct {
	PerPage int
	Page    int
	Order   Order
}
