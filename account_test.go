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
