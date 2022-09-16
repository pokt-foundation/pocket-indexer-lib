package indexer

import (
	testMock "github.com/stretchr/testify/mock"
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

func (w *writerMock) WriteAccounts(accounts []*Account) error {
	args := w.Called(accounts)

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
