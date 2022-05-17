package indexer

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/pokt-foundation/pocket-go/mock-client"
	"github.com/pokt-foundation/pocket-go/provider"
	testMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type writerMock struct {
	testMock.Mock
}

func (w *writerMock) WriteBlock(block *Block) error {
	args := w.Called(block)

	return args.Error(0)
}

func (w *writerMock) WriteTransactions(txs []*Transaction) error {
	args := w.Called(txs)

	return args.Error(0)
}

func TestIndexer_IndexBlockTransactions(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	writerMock := &writerMock{}

	indexer := NewIndexer(reqProvider, writerMock)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockTXsRoute),
		http.StatusInternalServerError, "samples/query_block_txs.json")

	err := indexer.IndexBlockTransactions(30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockTXsRoute),
		http.StatusOK, "samples/query_block_txs_empty.json")

	err = indexer.IndexBlockTransactions(30363)
	c.Equal(ErrNoTransactionsToIndex, err)

	mock.AddMultipleMockedResponses(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockTXsRoute),
		http.StatusOK, []string{
			"samples/query_block_txs.json",
			"samples/query_block_txs_empty.json",
			"samples/query_block_txs.json",
			"samples/query_block_txs_empty.json",
		})

	writerMock.On("WriteTransactions", testMock.Anything).Return(errors.New("forced failure")).Once()

	err = indexer.IndexBlockTransactions(30363)
	c.EqualError(err, "forced failure")

	writerMock.On("WriteTransactions", testMock.Anything).Return(nil).Once()

	err = indexer.IndexBlockTransactions(30363)
	c.NoError(err)
}

func TestIndexer_IndexBlock(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	writerMock := &writerMock{}

	indexer := NewIndexer(reqProvider, writerMock)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusInternalServerError, "samples/query_block.json")

	err := indexer.IndexBlock(30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusOK, "samples/query_block_empty.json")

	err = indexer.IndexBlock(30363)
	c.Equal(ErrBlockHasNoHash, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusOK, "samples/query_block.json")

	writerMock.On("WriteBlock", testMock.Anything).Return(errors.New("forced failure")).Once()

	err = indexer.IndexBlock(30363)
	c.EqualError(err, "forced failure")

	writerMock.On("WriteBlock", testMock.Anything).Return(nil).Once()

	err = indexer.IndexBlock(30363)
	c.NoError(err)
}
