package indexer

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/pokt-foundation/pocket-go/mock-client"
	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/stretchr/testify/require"
)

var forceWriterFail bool

type dummyWriter struct{}

func (w dummyWriter) WriteBlock(block *Block) error {
	if forceWriterFail {
		return errors.New("forced failure")
	}

	return nil
}

func (w dummyWriter) WriteTransactions(txs []*Transaction) error {
	if forceWriterFail {
		return errors.New("forced failure")
	}

	return nil
}

func TestIndexer_IndexBlockTransactions(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	indexer := NewIndexer(reqProvider, dummyWriter{})

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockTXsRoute),
		http.StatusInternalServerError, "samples/query_block_txs.json")

	err := indexer.IndexBlockTransactions(30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockTXsRoute),
		http.StatusOK, "samples/query_block_txs.json")

	forceWriterFail = true

	err = indexer.IndexBlockTransactions(30363)
	c.EqualError(err, "forced failure")

	forceWriterFail = false

	err = indexer.IndexBlockTransactions(30363)
	c.NoError(err)
}

func TestIndexer_IndexBlock(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	indexer := NewIndexer(reqProvider, dummyWriter{})

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusInternalServerError, "samples/query_block.json")

	err := indexer.IndexBlock(30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusOK, "samples/query_block.json")

	forceWriterFail = true

	err = indexer.IndexBlock(30363)
	c.EqualError(err, "forced failure")

	forceWriterFail = false

	err = indexer.IndexBlock(30363)
	c.NoError(err)
}
