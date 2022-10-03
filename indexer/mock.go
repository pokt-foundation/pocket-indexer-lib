package indexer

import (
	"github.com/pokt-foundation/pocket-indexer-lib/types"
	testMock "github.com/stretchr/testify/mock"
)

type driverMock struct {
	testMock.Mock
}

func (d *driverMock) WriteBlock(block *types.Block) error {
	args := d.Called(block)

	return args.Error(0)
}

func (d *driverMock) WriteTransactions(txs []*types.Transaction) error {
	args := d.Called(txs)

	return args.Error(0)
}

func (d *driverMock) WriteAccounts(accounts []*types.Account) error {
	args := d.Called(accounts)

	return args.Error(0)
}

func (d *driverMock) WriteNodes(nodes []*types.Node) error {
	args := d.Called(nodes)

	return args.Error(0)
}

func (d *driverMock) WriteApps(apps []*types.App) error {
	args := d.Called(apps)

	return args.Error(0)
}

func (d *driverMock) GetAccountsQuantity(options *types.GetAccountsQuantityOptions) (int64, error) {
	args := d.Called(options)

	return args.Get(0).(int64), args.Error(1)
}

func (d *driverMock) GetAppsQuantity(options *types.GetAppsQuantityOptions) (int64, error) {
	args := d.Called(options)

	return args.Get(0).(int64), args.Error(1)
}

func (d *driverMock) GetNodesQuantity(options *types.GetNodesQuantityOptions) (int64, error) {
	args := d.Called(options)

	return args.Get(0).(int64), args.Error(1)
}

func (d *driverMock) ReadBlockByHeight(height int) (*types.Block, error) {
	args := d.Called(height)

	return args.Get(0).(*types.Block), args.Error(1)
}

func (d *driverMock) WriteBlockCalculatedFields(block *types.Block) error {
	args := d.Called(block)

	return args.Error(0)
}
