package indexer

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
	"github.com/pokt-foundation/utils-go/mock-client"
	testMock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestIndexer_IndexBlock(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	driverMock := &driverMock{}

	indexer := NewIndexer(reqProvider, driverMock)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusInternalServerError, "../samples/query_block.json")

	err := indexer.IndexBlock(30363)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusOK, "../samples/query_block_empty.json")

	err = indexer.IndexBlock(30363)
	c.Equal(ErrBlockHasNoHash, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryBlockRoute),
		http.StatusOK, "../samples/query_block.json")

	driverMock.On("WriteBlock", testMock.Anything).Return(errors.New("forced failure")).Once()

	err = indexer.IndexBlock(30363)
	c.EqualError(err, "forced failure")

	driverMock.On("WriteBlock", testMock.Anything).Return(nil).Once()

	err = indexer.IndexBlock(30363)
	c.NoError(err)
}

func TestIndexer_IndexBlockWithCalculatedFields(t *testing.T) {
	c := require.New(t)

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	driverMock := &driverMock{}

	indexer := NewIndexer(reqProvider, driverMock)

	driverMock.On("GetAccountsQuantity", testMock.Anything).Return(int64(0), errors.New("error on accs")).Once()

	err := indexer.IndexBlockCalculatedFields(30363, true)
	c.EqualError(err, "error on accs")

	driverMock.On("GetAccountsQuantity", testMock.Anything).Return(int64(21), nil)
	driverMock.On("GetAppsQuantity", testMock.Anything).Return(int64(0), errors.New("error on apps")).Once()

	err = indexer.IndexBlockCalculatedFields(30363, true)
	c.EqualError(err, "error on apps")

	driverMock.On("GetAppsQuantity", testMock.Anything).Return(int64(21), nil)
	driverMock.On("GetNodesQuantity", testMock.Anything).Return(int64(0), errors.New("error on nodes")).Once()

	err = indexer.IndexBlockCalculatedFields(30363, true)
	c.EqualError(err, "error on nodes")

	driverMock.On("GetNodesQuantity", testMock.Anything).Return(int64(21), nil)
	driverMock.On("ReadBlockByHeight", 30363-1).Return(&types.Block{}, errors.New("error on last block")).Once()

	err = indexer.IndexBlockCalculatedFields(30363, true)
	c.EqualError(err, "error on last block")

	driverMock.On("ReadBlockByHeight", 30363-1).Return(&types.Block{
		Time: time.Now(),
	}, nil)
	driverMock.On("ReadBlockByHeight", 30363).Return(&types.Block{}, errors.New("error on height block")).Once()

	err = indexer.IndexBlockCalculatedFields(30363, true)
	c.EqualError(err, "error on height block")

	driverMock.On("ReadBlockByHeight", 30363).Return(&types.Block{
		Time: time.Now(),
	}, nil)
	driverMock.On("WriteBlockCalculatedFields", testMock.Anything).Return(errors.New("error on writing")).Once()

	err = indexer.IndexBlockCalculatedFields(30363, true)
	c.EqualError(err, "error on writing")

	driverMock.On("WriteBlockCalculatedFields", testMock.Anything).Return(nil)

	err = indexer.IndexBlockCalculatedFields(30363, true)
	c.NoError(err)

	err = indexer.IndexBlockCalculatedFields(1, true)
	c.NoError(err)
}
