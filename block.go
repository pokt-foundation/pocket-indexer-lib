package indexer

import (
	"errors"
	"strconv"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
)

var (
	// ErrBlockHasNoHash error when block hash no hash
	ErrBlockHasNoHash = errors.New("block to index has no hash")
)

// Block struct handler of all block fields to be indexed
type Block struct {
	Hash            string
	Height          int
	Time            time.Time
	ProposerAddress string
	TXCount         int
}

func convertProviderBlockToBlock(providerBlock *provider.GetBlockOutput) *Block {
	blockHeader := providerBlock.Block.Header

	height, _ := strconv.Atoi(blockHeader.Height)
	totalTxs, _ := strconv.Atoi(blockHeader.TotalTxs)

	return &Block{
		Hash:            providerBlock.BlockID.Hash,
		Height:          height,
		Time:            blockHeader.Time,
		ProposerAddress: blockHeader.ProposerAddress,
		TXCount:         totalTxs,
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

	return i.writer.WriteBlock(convertProviderBlockToBlock(blockOutput))
}
