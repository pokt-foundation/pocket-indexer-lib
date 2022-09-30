// Package indexer has the functions to get node values from a node and save them with a Writer interface
package indexer

import (
	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlock(blockNumber int) (*provider.GetBlockOutput, error)
	GetBlockTransactions(options *provider.GetBlockTransactionsOptions) (*provider.GetBlockTransactionsOutput, error)
	GetAccounts(options *provider.GetAccountsOptions) (*provider.GetAccountsOutput, error)
	GetNodes(options *provider.GetNodesOptions) (*provider.GetNodesOutput, error)
	GetApps(options *provider.GetAppsOptions) (*provider.GetAppsOutput, error)
}

// Driver interface for driver methods needed to index
type Driver interface {
	WriteBlock(block *types.Block) error
	WriteTransactions(txs []*types.Transaction) error
	WriteAccounts(accounts []*types.Account) error
	WriteNodes(nodes []*types.Node) error
	WriteApps(apps []*types.App) error
}

// Indexer struct handler for Indexer functions
type Indexer struct {
	provider Provider
	driver   Driver
}

// NewIndexer returns Indexer instance with given input
func NewIndexer(provider Provider, writer Driver) *Indexer {
	return &Indexer{
		provider: provider,
		driver:   writer,
	}
}
