package indexer

type Provider interface {
	GetBlockHeight() (int, error)
	// TODO: add missing functions
}

type Database interface {
	// TODO: add missing functions
}

type Indexer struct {
	provider Provider
	database Database
}

func NewIndexer(provider Provider, database Database) *Indexer {
	return &Indexer{
		provider: provider,
		database: database,
	}
}

func (i *Indexer) IndexBlock(block int) error {
	return nil
}
