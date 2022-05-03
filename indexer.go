package indexer

// Provider interface of needed provider functions
type Provider interface {
	GetBlockHeight() (int, error)
	// TODO: add missing functions
}

// Writer interface of write function
type Writer interface {
	// TODO: add missing functions
}

// Indexer struc handler for Indexer functions
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

// IndexBlock transfers all transactions to a known strcuture and saves them
func (i *Indexer) IndexBlock(blockHeight int) error {
	return nil
}
