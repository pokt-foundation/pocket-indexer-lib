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

func (w *writerMock) WriteAccount(account *Account) error {
	args := w.Called(account)

	return args.Error(0)
}

func (w *writerMock) WriteNodes(nodes []*Node) error {
	args := w.Called(nodes)

	return args.Error(0)
}

func (w *writerMock) WriteApps(apps []*App) error {
	args := w.Called(apps)

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

func TestIndexer_IndexAccount(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	writerMock := &writerMock{}

	indexer := NewIndexer(reqProvider, writerMock)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryAccountRoute),
		http.StatusInternalServerError, "samples/query_account.json")

	err := indexer.IndexAccount("ABCD", 30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryAccountRoute),
		http.StatusOK, "samples/query_account.json")

	writerMock.On("WriteAccount", testMock.Anything).Return(errors.New("forced failure")).Once()

	err = indexer.IndexAccount("ABCD", 30363)
	c.EqualError(err, "forced failure")

	writerMock.On("WriteAccount", testMock.Anything).Return(nil).Once()

	err = indexer.IndexAccount("ABCD", 30363)
	c.NoError(err)
}

func TestIndexer_IndexNodes(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	writerMock := &writerMock{}

	indexer := NewIndexer(reqProvider, writerMock)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryNodesRoute),
		http.StatusInternalServerError, "samples/query_nodes.json")

	err := indexer.IndexNodes(30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryNodesRoute),
		http.StatusOK, "samples/query_nodes_empty.json")

	err = indexer.IndexNodes(30363)
	c.Equal(ErrNoNodesToIndex, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryNodesRoute),
		http.StatusOK, "samples/query_nodes.json")

	writerMock.On("WriteNodes", testMock.Anything).Return(errors.New("forced failure")).Once()

	err = indexer.IndexNodes(30363)
	c.EqualError(err, "forced failure")

	writerMock.On("WriteNodes", testMock.Anything).Return(nil).Once()

	err = indexer.IndexNodes(30363)
	c.NoError(err)
}

func TestIndexer_IndexApps(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	writerMock := &writerMock{}

	indexer := NewIndexer(reqProvider, writerMock)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryAppsRoute),
		http.StatusInternalServerError, "samples/query_apps.json")

	err := indexer.IndexApps(30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryAppsRoute),
		http.StatusOK, "samples/query_apps_empty.json")

	err = indexer.IndexApps(30363)
	c.Equal(ErrNoAppsToIndex, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryAppsRoute),
		http.StatusOK, "samples/query_apps.json")

	writerMock.On("WriteApps", testMock.Anything).Return(errors.New("forced failure")).Once()

	err = indexer.IndexApps(30363)
	c.EqualError(err, "forced failure")

	writerMock.On("WriteApps", testMock.Anything).Return(nil).Once()

	err = indexer.IndexApps(30363)
	c.NoError(err)
}
