package indexer

import (
	"errors"
	"strconv"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
)

var (
	// ErrNoTransactionsToIndex error when there are no transactions to index
	ErrNoTransactionsToIndex = errors.New("no transactions to index")
	// ErrBlockHasNoHash error when block hash no hash
	ErrBlockHasNoHash = errors.New("block to index has no hash")
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlock(blockNumber int) (*provider.GetBlockOutput, error)
	GetBlockTransactions(blockHeight int, options *provider.GetBlockTransactionsOptions) (*provider.GetBlockTransactionsOutput, error)
}

// Writer interface for write methods to index
type Writer interface {
	WriteBlock(block *Block) error
	WriteTransactions(txs []*Transaction) error
}

// Indexer struct handler for Indexer functions
type Indexer struct {
	provider Provider
	writer   Writer
}

// NewIndexer returns Indexer instance with given input
func NewIndexer(provider Provider, writer Writer) *Indexer {
	return &Indexer{
		provider: provider,
		writer:   writer,
	}
}

// Transaction struct handler of all transaction fields to be indexed
type Transaction struct {
	Hash            string
	FromAddress     string
	ToAddress       string
	AppPubKey       string
	Blockchains     []string
	MessageType     string
	Height          int
	Index           int
	StdTx           *provider.StdTx
	TxResult        *provider.TxResult
	Tx              string
	Entropy         int
	Fee             int
	FeeDenomination string
	Amount          int
}

func convertProviderTransactionToTransaction(providerTransaction *provider.Transaction) *Transaction {
	var fromAddress, toAddress string
	var amount int
	var blockChains []string

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
		amount, _ = strconv.Atoi(rawAmount)
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

	return &Transaction{
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
		blockTransactionsOutput, err := i.provider.GetBlockTransactions(blockHeight, &provider.GetBlockTransactionsOptions{
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

	var transactions []*Transaction

	for _, tx := range providerTxs {
		transactions = append(transactions, convertProviderTransactionToTransaction(tx))
	}

	err := i.writer.WriteTransactions(transactions)
	if err != nil {
		return err
	}

	return nil
}

// Block struct handler of all block fields to be indexed
type Block struct {
	Hash            string
	Height          int
	Time            time.Time
	ProposerAddress string
	TXCount         int
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

	err = i.writer.WriteBlock(convertProviderBlockToBlock(blockOutput))
	if err != nil {
		return err
	}

	return nil
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
