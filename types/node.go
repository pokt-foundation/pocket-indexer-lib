package types

import "math/big"

// Node struct handler of all node fields to be indexed
type Node struct {
	Address    string
	Height     int
	Jailed     bool
	PublicKey  string
	ServiceURL string
	Tokens     *big.Int
}

// ReadNodeByAddressOptions optional parameters for ReadNodeByAddress
type ReadNodeByAddressOptions struct {
	Height int
}

// ReadNodesOptions optional parameters for ReadNodes
type ReadNodesOptions struct {
	PerPage int
	Page    int
	Height  int
}

// GetNodesQuantityOptions optinal params for GetNodesQuantity
type GetNodesQuantityOptions struct {
	Height int
}
