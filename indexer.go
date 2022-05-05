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

// Persistance layer interface (database, in-memory, etc.)
type Persistance interface {
	// Writes
	WriteBlock(block *provider.GetBlockOutput) error
	WriteTransactions(txs []*provider.Transaction) error
	// Reads
	ReadBlock(blockHeight int) (interface{}, error)
	ReadTransaction(hash string) (interface{}, error)
	ReadBlockTransactions(blockHeight int) ([]interface{}, error)
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
	blockOutput, err := i.provider.GetBlock(blockHeight)

	if err != nil {
		return err
	}

	writeErr := i.persistance.WriteBlock(blockOutput)

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

	txs := make([]*provider.Transaction, 0)

	for _, transaction := range blockTransactionsOutput.Txs {
		txs = append(txs, transaction)
	}

	writeErr := i.persistance.WriteTransactions(txs)

	if writeErr != nil {
		return writeErr
	}

	return nil
}
