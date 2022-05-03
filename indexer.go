package indexer

// Provider interface of needed provider functions
type Provider interface {
	GetBlockHeight() (int, error)
	// TODO: add missing functions
}

// Database interface of needed database functions
type Database interface {
	// TODO: add missing functions
}

// Indexer struc handler for Indexer functions
type Indexer struct {
	provider Provider
	database Database
}

// NewIndexer returns Indexer instance with given input
func NewIndexer(provider Provider, database Database) *Indexer {
	return &Indexer{
		provider: provider,
		database: database,
	}
}

// IndexBlock transfers all transactions to a known strcuture and saves them
func (i *Indexer) IndexBlock(block int) error {
	return nil
}
