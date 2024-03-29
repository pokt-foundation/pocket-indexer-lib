package indexer

import (
	"errors"
	"math/big"
	"strconv"

	"github.com/pokt-foundation/pocket-go/provider"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

var (
	// ErrNoTransactionsToIndex error when there are no transactions to index
	ErrNoTransactionsToIndex = errors.New("no transactions to index")
)

func convertProviderTransactionToTransaction(providerTransaction *provider.Transaction) *types.Transaction {
	var fromAddress, toAddress string
	var blockChains []string

	amount := new(big.Int)

	stdTx := providerTransaction.StdTx
	msgValues := stdTx.Msg.Value
	feeStruct := stdTx.Fee[0]

	rawFromAddress, ok := msgValues["from_address"].(string)
	if ok {
		fromAddress = rawFromAddress
	}

	rawToAddress, ok := msgValues["to_address"].(string)
	if ok {
		toAddress = rawToAddress
	}

	rawAmount, ok := msgValues["amount"].(string)
	if ok {
		amount, _ = amount.SetString(rawAmount, 10)
	}

	rawBlockChains, ok := msgValues["chains"].([]any)
	if ok {
		for _, rawBlockChain := range rawBlockChains {
			blockChain, ok := rawBlockChain.(string)
			if ok {
				blockChains = append(blockChains, blockChain)
			}
		}
	}

	fee, _ := strconv.Atoi(feeStruct.Amount)

	return &types.Transaction{
		Hash:            providerTransaction.Hash,
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		AppPubKey:       stdTx.Signature.PubKey,
		Blockchains:     blockChains,
		MessageType:     stdTx.Msg.Type,
		Height:          providerTransaction.Height,
		Index:           providerTransaction.Index,
		StdTx:           stdTx,
		TxResult:        providerTransaction.TxResult,
		Tx:              providerTransaction.Tx,
		Entropy:         int(stdTx.Entropy),
		Fee:             fee,
		FeeDenomination: feeStruct.Denom,
		Amount:          amount,
	}
}

// IndexBlockTransactions converts block transactions to a known structure and saves them
func (i *Indexer) IndexBlockTransactions(blockHeight int) error {
	currentPage := 1
	var providerTxs []*provider.Transaction

	for {
		blockTransactionsOutput, err := i.provider.GetBlockTransactions(&provider.GetBlockTransactionsOptions{
			Height:  blockHeight,
			Page:    currentPage,
			PerPage: 10000,
		})
		if err != nil {
			return err
		}

		if blockTransactionsOutput.PageCount == 0 {
			break
		}

		providerTxs = append(providerTxs, blockTransactionsOutput.Txs...)

		currentPage++
	}

	if len(providerTxs) == 0 {
		return ErrNoTransactionsToIndex
	}

	var transactions []*types.Transaction

	for _, tx := range providerTxs {
		transactions = append(transactions, convertProviderTransactionToTransaction(tx))
	}

	return i.driver.WriteTransactions(transactions)
}
