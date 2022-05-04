package indexer

import (
	"github.com/pokt-foundation/pocket-indexer/types"
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlockHeight() (int, error)
	// TODO: add missing functions
}

// Persistance layer interface (database, in-memory, etc.)
type Persistance interface {
	// Writes
	WriteBlock(block *types.Block) error
	WriteTransactions(txs []*types.Transaction) error
	// Reads
	ReadBlock(blockHeight int) (*types.Block, error)
	ReadBlockTransactions(blockHeight int) ([]*types.Transaction, error)
	ReadTransaction(hash string) (*types.Transaction, error)
}

// Indexer struc handler for Indexer functions
type Indexer struct {
	provider    Provider
	persistance Persistance
}

// NewIndexer returns Indexer instance with given input
func NewIndexer(provider Provider, persistance Persistance) *Indexer {
	return &Indexer{
		provider:    provider,
		persistance: persistance,
	}
}

// IndexBlock converts block details to a known structure and saves them
func (i *Indexer) IndexBlock(blockHeight int) error {
	return nil
}

// IndexBlock converts block transactions to a known structure and saves them
func (i *Indexer) IndexBlockTransactions(blockHeight int) error {
	return nil
}
