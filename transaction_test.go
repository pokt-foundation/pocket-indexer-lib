package indexer

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/utils-go/mock-client"
	testMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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
