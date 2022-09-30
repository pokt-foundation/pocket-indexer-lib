package indexer

import (
	"github.com/pokt-foundation/pocket-indexer-lib/types"
	testMock "github.com/stretchr/testify/mock"
)

type writerMock struct {
	testMock.Mock
}

func (w *writerMock) WriteBlock(block *types.Block) error {
	args := w.Called(block)

	return args.Error(0)
}

func (w *writerMock) WriteTransactions(txs []*types.Transaction) error {
	args := w.Called(txs)

	return args.Error(0)
}

func (w *writerMock) WriteAccounts(accounts []*types.Account) error {
	args := w.Called(accounts)

	return args.Error(0)
}

func (w *writerMock) WriteNodes(nodes []*types.Node) error {
	args := w.Called(nodes)

	return args.Error(0)
}

func (w *writerMock) WriteApps(apps []*types.App) error {
	args := w.Called(apps)

	return args.Error(0)
}
