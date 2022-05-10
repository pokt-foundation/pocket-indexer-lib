package indexer

import (
	"strconv"

	"github.com/pokt-foundation/pocket-go/provider"
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlockHeight() (int, error)
	GetBlock(blockNumber int) (*provider.GetBlockOutput, error)
	GetBlockTransactions(blockHeight int, options *provider.GetBlockTransactionsOptions) (*provider.GetBlockTransactionsOutput, error)
}

// Writer interface for write methods to index
type Writer interface {
	// Writes
	WriteBlock(block *Block) error
	WriteTransactions(txs []*Transaction) error
}

// Indexer struc handler for Indexer functions
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
	ID              int
	Hash            string
	FromAddress     string
	ToAddress       string
	AppPubKey       string
	Blockchains     []string
	MessageType     string
	Height          int
	Index           int
	Proof           *provider.TransactionProof
	StdTx           *provider.StdTx
	TxResult        *provider.TxResult
	Tx              string
	Entropy         int
	Fee             int
	FeeDenomination string
}

func convertProvTransactionToTransaction(provTransaction *provider.Transaction) *Transaction {
	var fromAddress, toAddress string
	var blockChains []string

	stdTx := provTransaction.StdTx
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
		Hash:            provTransaction.Hash,
		FromAddress:     fromAddress,
		ToAddress:       toAddress,
		AppPubKey:       stdTx.Signature.PubKey,
		Blockchains:     blockChains,
		MessageType:     stdTx.Msg.Type,
		Height:          provTransaction.Height,
		Index:           provTransaction.Index,
		Proof:           provTransaction.Proof,
		StdTx:           stdTx,
		TxResult:        provTransaction.TxResult,
		Tx:              provTransaction.Tx,
		Entropy:         int(stdTx.Entropy),
		Fee:             fee,
		FeeDenomination: feeStruct.Denom,
	}
}

// IndexBlockTransactions converts block transactions to a known structure and saves them
func (i *Indexer) IndexBlockTransactions(blockHeight int) error {
	// TODO: add pagination support
	blockTransactionsOutput, err := i.provider.GetBlockTransactions(blockHeight, nil)
	if err != nil {
		return err
	}

	var transactions []*Transaction

	for _, tx := range blockTransactionsOutput.Txs {
		transactions = append(transactions, convertProvTransactionToTransaction(tx))
	}

	err = i.writer.WriteTransactions(transactions)
	if err != nil {
		return err
	}

	return nil
}

// Block struct handler of all block field to be indexed
// TODO: implement this struct
type Block struct{}

// IndexBlock converts block details to a known structure and saves them
func (i *Indexer) IndexBlock(blockHeight int) error {
	blockOutput, err := i.provider.GetBlock(blockHeight)
	if err != nil {
		return err
	}

	err = i.writer.WriteBlock(convertProvBlockToBlock(blockOutput))
	if err != nil {
		return err
	}

	return nil
}

// TODO: implement this function correctly
func convertProvBlockToBlock(provBlock *provider.GetBlockOutput) *Block {
	return &Block{}
}
