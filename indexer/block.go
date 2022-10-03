package indexer

import (
	"errors"
	"strconv"
	"time"

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

func (i *Indexer) IndexBlockCalculatedFields(blockHeight int, getTook bool) error {
	accountsQuantity, err := i.driver.GetAccountsQuantity(&types.GetAccountsQuantityOptions{
		Height: blockHeight,
	})
	if err != nil {
		return err
	}

	appsQuantity, err := i.driver.GetAppsQuantity(&types.GetAppsQuantityOptions{
		Height: blockHeight,
	})
	if err != nil {
		return err
	}

	nodesQuantity, err := i.driver.GetNodesQuantity(&types.GetNodesQuantityOptions{
		Height: blockHeight,
	})
	if err != nil {
		return err
	}

	var took time.Duration

	if getTook {
		took, err = i.getDuration(blockHeight)
		if err != nil {
			return err
		}
	}

	return i.driver.WriteBlockCalculatedFields(&types.Block{
		Height:           blockHeight,
		AccountsQuantity: int(accountsQuantity),
		AppsQuantity:     int(appsQuantity),
		NodesQuantity:    int(nodesQuantity),
		Took:             took,
	})
}

func (i *Indexer) getDuration(blockHeight int) (time.Duration, error) {
	if blockHeight == 1 {
		return 0, nil
	}

	lastBlock, err := i.driver.ReadBlockByHeight(blockHeight - 1)
	if err != nil {
		return 0, err
	}

	heightBlock, err := i.driver.ReadBlockByHeight(blockHeight)
	if err != nil {
		return 0, err
	}

	return heightBlock.Time.Sub(lastBlock.Time), nil
}
