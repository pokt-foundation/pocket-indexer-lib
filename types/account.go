package types

import "math/big"

// Account struct handler of all account fields to be indexed
type Account struct {
	Address             string
	Height              int
	Balance             *big.Int
	BalanceDenomination string
}

// ReadAccountByAddressOptions optional parameters for ReadAccountByAddress
type ReadAccountByAddressOptions struct {
	Height int
}

// ReadAccountsOptions optional parameters for ReadAccounts
type ReadAccountsOptions struct {
	PerPage int
	Page    int
	Height  int
}

// GetAccountsQuantityOptions optional parameters for GetAccountsQuantity
type GetAccountsQuantityOptions struct {
	Height int
}
