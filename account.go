package indexer

import (
	"math/big"

	"github.com/pokt-foundation/pocket-go/provider"
)

// AccountType enum of possible account types
type AccountType string

const (
	// AccountTypeNode represents node account type
	AccountTypeNode AccountType = "node"
	// AccountTypeApp represents app account type
	AccountTypeApp AccountType = "app"
)

// Account struct handler of all account fields to be indexed
type Account struct {
	Address             string
	Height              int
	AccountType         AccountType
	Balance             *big.Int
	BalanceDenomination string
}

func convertProviderAccountToAccount(height int, accountType AccountType, providerAccount *provider.GetAccountOutput) *Account {
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
		AccountType:         accountType,
		Balance:             balance,
		BalanceDenomination: balanceDenomination,
	}
}

// IndexAccount converts account details to a known structure and saves them
func (i *Indexer) IndexAccount(address string, blockHeight int, accountType AccountType) error {
	accountOutput, err := i.provider.GetAccount(address, &provider.GetAccountOptions{Height: blockHeight})
	if err != nil {
		return err
	}

	return i.writer.WriteAccount(convertProviderAccountToAccount(blockHeight, accountType, accountOutput))
}
