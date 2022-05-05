package indexer

import (
	"strconv"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlockHeight() (int, error)
	GetBlock(blockHeight int) (*provider.GetBlockOutput, error)
	GetBlockTransactions(blockHeight int) (*provider.GetBlockTransactionsOutput, error)
}

// Persistance layer interface (database, in-memory, etc.)
type Persistance interface {
	// Writes
	WriteBlock(block *types.Block) error
	WriteTransactions(txs []*types.Transaction) error
	// Reads
	ReadBlock(blockHeight int) (*types.Block, error)
	ReadBlockTransactions(blockHeight int) ([]*types.Transaction, error)
	ReadTransaction(hash string) (*types.Transaction, error)
}

// Indexer struc handler for Indexer functions
type Indexer struct {
	provider    Provider
	persistance Persistance
}

// NewIndexer returns Indexer instance with given input
func NewIndexer(provider Provider, persistance Persistance) *Indexer {
	return &Indexer{
		provider:    provider,
		persistance: persistance,
	}
}

// IndexBlock converts block details to a known structure and saves them
func (i *Indexer) IndexBlock(blockHeight int) (*types.Block, error) {
	blockOutput, err := i.provider.GetBlock(blockHeight)

	if err != nil {
		return nil, err
	}

	totalTxs, err := strconv.Atoi(blockOutput.Block.Header.TotalTxs)

	block := &types.Block{
		Height:    blockHeight,
		TxCount:   totalTxs,
		Producer:  blockOutput.Block.Header.ProposerAddress,
		Timestamp: blockOutput.Block.Header.Time,
	}

	err = i.persistance.WriteBlock(block)

	if err != nil {
		return nil, err
	}

	return block, nil
}

// IndexBlock converts block transactions to a known structure and saves them
func (i *Indexer) IndexBlockTransactions(blockHeight int) ([]*types.Transaction, error) {
	blockTransactionsOutput, err := i.provider.GetBlockTransactions(blockHeight)

	if err != nil {
		return nil, err
	}

	var status = "SUCCESS"

	txs := make([]*types.Transaction, 0)

	for _, transaction := range blockTransactionsOutput.Txs {

		if transaction.TxResult.Code != 0 {
			status = "FAILURE"
		}

		val, ok := blockTransactionsOutput.Txs[0].StdTx.Msg.Value["amount"]

		if !ok {
			return nil, nil
		}

		amount := val.(string)

		fee := transaction.StdTx.Fee[0].Amount

		tx := &types.Transaction{
			Status:   status,
			Hash:     transaction.Hash,
			Sender:   transaction.TxResult.Signer,
			Receiver: transaction.TxResult.Recipient,
			Height:   transaction.Height,
			Code:     transaction.TxResult.Code,
			Fee:      fee,
			Amount:   amount,
			Memo:     transaction.StdTx.Memo,
		}

		txs = append(txs, tx)
	}

	err = i.persistance.WriteTransactions(txs)

	if err != nil {
		return nil, err
	}

	return txs, nil
}
