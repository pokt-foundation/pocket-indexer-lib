package indexer

import (
	"errors"
	"math/big"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

var (
	// ErrNoNodesToIndex error when there are no nodes to index
	ErrNoNodesToIndex = errors.New("no nodes to index")
)

func convertProviderNodeToNode(height int, provNode *provider.Node) *types.Node {
	tokens := new(big.Int)
	tokens, _ = tokens.SetString(provNode.Tokens, 10)

	return &types.Node{
		Address:    provNode.Address,
		Height:     height,
		Jailed:     provNode.Jailed,
		PublicKey:  provNode.PublicKey,
		ServiceURL: provNode.ServiceURL,
		Tokens:     tokens,
	}
}

// IndexBlockNodes converts nodes details to known structures and saves them
// returns all addresses indexed
func (i *Indexer) IndexBlockNodes(blockHeight int) ([]string, error) {
	totalPages := 1
	var providerNodes []*provider.Node

	for page := 1; page <= totalPages; page++ {
		nodesOutput, err := i.provider.GetNodes(&provider.GetNodesOptions{
			Height:  blockHeight,
			Page:    page,
			PerPage: 10000,
		})
		if err != nil {
			return nil, err
		}

		if page == 1 {
			totalPages = nodesOutput.TotalPages
		}

		providerNodes = append(providerNodes, nodesOutput.Result...)
	}

	if len(providerNodes) == 0 {
		return nil, ErrNoNodesToIndex
	}

	var nodes []*types.Node
	var addresses []string

	for _, node := range providerNodes {
		nodes = append(nodes, convertProviderNodeToNode(blockHeight, node))
		addresses = append(addresses, node.Address)
	}

	return addresses, i.driver.WriteNodes(nodes)
}
