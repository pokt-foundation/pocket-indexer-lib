package indexer

import (
	"errors"
	"math/big"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

var (
	// ErrNoAccountsToIndex error when there are no accounts to index
	ErrNoAccountsToIndex = errors.New("no accounts to index")
)

func convertProviderAccountToAccount(height int, providerAccount *provider.GetAccountOutput) *types.Account {
	var balanceDenomination string
	balance := new(big.Int)

	if len(providerAccount.Coins) == 1 {
		coins := providerAccount.Coins[0]
		balance, _ = balance.SetString(coins.Amount, 10)
		balanceDenomination = coins.Denom
	}

	return &types.Account{
		Address:             providerAccount.Address,
		Height:              height,
		Balance:             balance,
		BalanceDenomination: balanceDenomination,
	}
}

// IndexAccounts converts accounts details to known structures and saved them
// returns all addresses indexed
func (i *Indexer) IndexAccounts(blockHeight int) ([]string, error) {
	totalPages := 1
	var providerAccounts []*provider.GetAccountOutput

	for page := 1; page <= totalPages; page++ {
		accountsOutput, err := i.provider.GetAccounts(&provider.GetAccountsOptions{
			Height:  blockHeight,
			Page:    page,
			PerPage: 10000,
		})
		if err != nil {
			return nil, err
		}

		if page == 1 {
			totalPages = accountsOutput.TotalPages
		}

		providerAccounts = append(providerAccounts, accountsOutput.Result...)
	}

	if len(providerAccounts) == 0 {
		return nil, ErrNoAccountsToIndex
	}

	var accounts []*types.Account
	var addresses []string

	for _, account := range providerAccounts {
		accounts = append(accounts, convertProviderAccountToAccount(blockHeight, account))
		addresses = append(addresses, account.Address)
	}

	return addresses, i.driver.WriteAccounts(accounts)
}
