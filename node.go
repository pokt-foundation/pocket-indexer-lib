package indexer

import (
	"errors"
	"math/big"

	"github.com/pokt-foundation/pocket-go/provider"
)

var (
	// ErrNoNodesToIndex error when there are no nodes to index
	ErrNoNodesToIndex = errors.New("no nodes to index")
)

// Node struct handler of all node fields to be indexed
type Node struct {
	Address    string
	Height     int
	Jailed     bool
	PublicKey  string
	ServiceURL string
	Tokens     *big.Int
}

func convertProviderNodeToNode(height int, provNode *provider.Node) *Node {
	tokens := new(big.Int)
	tokens, _ = tokens.SetString(provNode.Tokens, 10)

	return &Node{
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

	var nodes []*Node
	var addresses []string

	for _, node := range providerNodes {
		nodes = append(nodes, convertProviderNodeToNode(blockHeight, node))
		addresses = append(addresses, node.Address)
	}

	return addresses, i.writer.WriteNodes(nodes)
}
