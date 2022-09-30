package indexer

import (
	"errors"
	"strconv"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

var (
	// ErrBlockHasNoHash error when block hash no hash
	ErrBlockHasNoHash = errors.New("block to index has no hash")
)

func convertProviderBlockToBlock(providerBlock *provider.GetBlockOutput) *types.Block {
	blockHeader := providerBlock.Block.Header

	height, _ := strconv.Atoi(blockHeader.Height)
	countTx, _ := strconv.Atoi(blockHeader.NumTxs)
	totalTxs, _ := strconv.Atoi(blockHeader.TotalTxs)

	return &types.Block{
		Hash:            providerBlock.BlockID.Hash,
		Height:          height,
		Time:            blockHeader.Time,
		ProposerAddress: blockHeader.ProposerAddress,
		TXCount:         countTx,
		TXTotal:         totalTxs,
	}
}

// IndexBlock converts block details to a known structure and saves them
func (i *Indexer) IndexBlock(blockHeight int) error {
	blockOutput, err := i.provider.GetBlock(blockHeight)
	if err != nil {
		return err
	}

	if blockOutput.BlockID.Hash == "" {
		return ErrBlockHasNoHash
	}

	return i.driver.WriteBlock(convertProviderBlockToBlock(blockOutput))
}
