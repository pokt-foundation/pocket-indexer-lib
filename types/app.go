package types

import "math/big"

// App struct handler of all app fields to be indexed
type App struct {
	Address      string
	Height       int
	Jailed       bool
	PublicKey    string
	StakedTokens *big.Int
}

// ReadAppByAddressOptions optional parameters for ReadAppByAddress
type ReadAppByAddressOptions struct {
	Height int
}

// ReadAppsOptions optional parameters for ReadApps
type ReadAppsOptions struct {
	PerPage int
	Page    int
	Height  int
}

// GetAppsQuantityOptions optinal params for GetAppsQuantity
type GetAppsQuantityOptions struct {
	Height int
}
