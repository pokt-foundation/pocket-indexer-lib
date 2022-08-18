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

func TestIndexer_IndexAccount(t *testing.T) {
	c := require.New(t)

	httpmock.Activate()
	defer httpmock.DeactivateAndReset()

	reqProvider := provider.NewProvider("https://dummy.com", []string{})

	writerMock := &writerMock{}

	indexer := NewIndexer(reqProvider, writerMock)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryAccountRoute),
		http.StatusInternalServerError, "samples/query_account.json")

	err := indexer.IndexAccount("ABCD", 30363, AccountTypeNode)
	c.Equal(provider.Err5xxOnConnection, err)

	mock.AddMockedResponseFromFile(http.MethodPost, fmt.Sprintf("%s%s", "https://dummy.com", provider.QueryAccountRoute),
		http.StatusOK, "samples/query_account.json")

	writerMock.On("WriteAccount", testMock.Anything).Return(errors.New("forced failure")).Once()

	err = indexer.IndexAccount("ABCD", 30363, AccountTypeNode)
	c.EqualError(err, "forced failure")

	writerMock.On("WriteAccount", testMock.Anything).Return(nil).Once()

	err = indexer.IndexAccount("ABCD", 30363, AccountTypeNode)
	c.NoError(err)
}
