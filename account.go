package indexer

import (
	"errors"
	"math/big"

	"github.com/pokt-foundation/pocket-go/provider"
)

var (
	// ErrNoAccountsToIndex error when there are no accounts to index
	ErrNoAccountsToIndex = errors.New("no accounts to index")
)

// Account struct handler of all account fields to be indexed
type Account struct {
	Address             string
	Height              int
	Balance             *big.Int
	BalanceDenomination string
}

func convertProviderAccountToAccount(height int, providerAccount *provider.GetAccountOutput) *Account {
	var balanceDenomination string
	balance := new(big.Int)

	if len(providerAccount.Coins) == 1 {
		coins := providerAccount.Coins[0]
		balance, _ = balance.SetString(coins.Amount, 10)
		balanceDenomination = coins.Denom
	}

	return &Account{
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

	var accounts []*Account
	var addresses []string

	for _, account := range providerAccounts {
		accounts = append(accounts, convertProviderAccountToAccount(blockHeight, account))
		addresses = append(addresses, account.Address)
	}

	return addresses, i.writer.WriteAccounts(accounts)
}
