// Package indexer has the functions to get node values from a node and save them with a Writer interface
package indexer

import (
	"github.com/pokt-foundation/pocket-go/provider"
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlock(blockNumber int) (*provider.GetBlockOutput, error)
	GetBlockTransactions(options *provider.GetBlockTransactionsOptions) (*provider.GetBlockTransactionsOutput, error)
	GetAccount(address string, options *provider.GetAccountOptions) (*provider.GetAccountOutput, error)
	GetNodes(options *provider.GetNodesOptions) (*provider.GetNodesOutput, error)
	GetApps(options *provider.GetAppsOptions) (*provider.GetAppsOutput, error)
}

// Writer interface for write methods to index
type Writer interface {
	WriteBlock(block *Block) error
	WriteTransactions(txs []*Transaction) error
	WriteAccount(account *Account) error
	WriteNodes(nodes []*Node) error
	WriteApps(apps []*App) error
}

// Indexer struct handler for Indexer functions
type Indexer struct {
	provider Provider
	writer   Writer
}

// NewIndexer returns Indexer instance with given input
func NewIndexer(provider Provider, writer Writer) *Indexer {
	return &Indexer{
		provider: provider,
		writer:   writer,
	}
}
