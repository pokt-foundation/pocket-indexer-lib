package indexer

import (
	"errors"
	"math/big"
	"strconv"
	"time"

	"github.com/pokt-foundation/pocket-go/provider"
)

var (
	// ErrNoTransactionsToIndex error when there are no transactions to index
	ErrNoTransactionsToIndex = errors.New("no transactions to index")
	// ErrNoNodesToIndex error when there are no nodes to index
	ErrNoNodesToIndex = errors.New("no nodes to index")
	// ErrNoAppsToIndex error when there are no apps to index
	ErrNoAppsToIndex = errors.New("no apps to index")
	// ErrBlockHasNoHash error when block hash no hash
	ErrBlockHasNoHash = errors.New("block to index has no hash")
)

// Provider interface of needed provider functions
type Provider interface {
	GetBlock(blockNumber int) (*provider.GetBlockOutput, error)
	GetBlockTransactions(options *provider.GetBlockTransactionsOptions) (*provider.GetBlockTransactionsOutput, error)
	GetAccount(address string, options *provider.GetAccountOptions) (*provider.GetAccountOutput, error)
	GetNodes(options *provider.GetNodesOptions) (*provider.GetNodesOutput, error)
	GetApps(options *provider.GetAppsOptions) (*provider.GetAppsOutput, error)
}

// Writer interface for write methods to index
type Writer interface {
	WriteBlock(block *Block) error
	WriteTransactions(txs []*Transaction) error
	WriteAccount(account *Account) error
	WriteNodes(nodes []*Node) error
	WriteApps(apps []*App) error
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

	var transactions []*Transaction

	for _, tx := range providerTxs {
		transactions = append(transactions, convertProviderTransactionToTransaction(tx))
	}

	return i.writer.WriteTransactions(transactions)
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

	return i.writer.WriteBlock(convertProviderBlockToBlock(blockOutput))
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

// Account struct handler of all account fields to be indexed
type Account struct {
	Address             string
	Height              int
	Balance             *big.Int
	BalanceDenomination string
}

// IndexAccount converts account details to a known structure and saves them
func (i *Indexer) IndexAccount(address string, blockHeight int) error {
	accountOutput, err := i.provider.GetAccount(address, &provider.GetAccountOptions{Height: blockHeight})
	if err != nil {
		return err
	}

	return i.writer.WriteAccount(convertProviderAccountToAccount(blockHeight, accountOutput))
}

func convertProviderAccountToAccount(height int, providerAccount *provider.GetAccountOutput) *Account {
	coins := providerAccount.Coins[0]

	balance := new(big.Int)
	balance, _ = balance.SetString(coins.Amount, 10)

	return &Account{
		Address:             providerAccount.Address,
		Height:              height,
		Balance:             balance,
		BalanceDenomination: coins.Denom,
	}
}

// Node struct handler of all node fields to be indexed
type Node struct {
	Address    string
	Height     int
	Jailed     bool
	PublicKey  string
	ServiceURL string
	Tokens     *big.Int
}

// IndexNodes converts nodes details to known structures and saves them
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

// App struct handler of all app fields to be indexed
type App struct {
	Address      string
	Height       int
	Jailed       bool
	PublicKey    string
	StakedTokens *big.Int
}

// IndexApps converts apps details to known structures and saved them
// returns all addresses indexed
func (i *Indexer) IndexBlockApps(blockHeight int) ([]string, error) {
	totalPages := 1
	var providerApps []*provider.App

	for page := 1; page <= totalPages; page++ {
		appsOutput, err := i.provider.GetApps(&provider.GetAppsOptions{
			Height:  blockHeight,
			Page:    page,
			PerPage: 10000,
		})
		if err != nil {
			return nil, err
		}

		if page == 1 {
			totalPages = appsOutput.TotalPages
		}

		providerApps = append(providerApps, appsOutput.Result...)
	}

	if len(providerApps) == 0 {
		return nil, ErrNoAppsToIndex
	}

	var apps []*App
	var addresses []string

	for _, app := range providerApps {
		apps = append(apps, convertProviderAppToApp(blockHeight, app))
		addresses = append(addresses, app.Address)
	}

	return addresses, i.writer.WriteApps(apps)
}

func convertProviderAppToApp(height int, provApp *provider.App) *App {
	stakedTokens := new(big.Int)
	stakedTokens, _ = stakedTokens.SetString(provApp.StakedTokens, 10)

	return &App{
		Address:      provApp.Address,
		Height:       height,
		Jailed:       provApp.Jailed,
		PublicKey:    provApp.PublicKey,
		StakedTokens: stakedTokens,
	}
}
