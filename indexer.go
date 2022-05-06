package indexer

import (
	"github.com/pokt-foundation/pocket-go/provider"
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlockHeight() (int, error)
	GetBlock(blockHeight int) (*provider.GetBlockOutput, error)
	GetBlockTransactions(blockHeight int) (*provider.GetBlockTransactionsOutput, error)
}

// Persistence layer interface (database, in-memory, etc.)
type Persistence interface {
	// Writes
	WriteBlock(block *provider.GetBlockOutput) error
	WriteTransactions(txs []*provider.Transaction) error
	// Reads
	ReadBlock(blockHeight int) (interface{}, error)
	ReadTransaction(hash string) (interface{}, error)
	ReadBlockTransactions(blockHeight int) (map[string]interface{}, error)
}

// Indexer struc handler for Indexer functions
type Indexer struct {
	provider    Provider
	persistence Persistence
}

// NewIndexer returns Indexer instance with given input
func NewIndexer(provider Provider, persistence Persistence) *Indexer {
	return &Indexer{
		provider:    provider,
		persistence: persistence,
	}
}

// IndexBlock converts block details to a known structure and saves them
func (i *Indexer) IndexBlock(blockHeight int) error {
	blockOutput, err := i.provider.GetBlock(blockHeight)

	if err != nil {
		return err
	}

	writeErr := i.persistence.WriteBlock(blockOutput)

	if writeErr != nil {
		return writeErr
	}

	return nil
}

// IndexBlockTransactions converts block transactions to a known structure and saves them
func (i *Indexer) IndexBlockTransactions(blockHeight int) error {
	blockTransactionsOutput, err := i.provider.GetBlockTransactions(blockHeight)

	if err != nil {
		return err
	}

	writeErr := i.persistence.WriteTransactions(blockTransactionsOutput.Txs)

	if writeErr != nil {
		return writeErr
	}

	return nil
}
