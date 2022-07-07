package postgresdriver

import (
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/pokt-foundation/pocket-go/utils"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
)

const (
	insertTransactionsScript = `
	INSERT into transactions (hash, from_address, to_address, app_pub_key, blockchains, message_type, height, index, stdtx, tx_result, tx, entropy, fee, fee_denomination, amount)
	(
		select * from unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::int[], $8::int[], $9::jsonb[], $10::jsonb[], $11::text[], $12::numeric[], $13::int[], $14::text[], $15::int[])
	)`
	insertBlockScript = `
	INSERT into blocks (hash, height, time, proposer_address, tx_count)
	VALUES (:hash, :height, :time, :proposer_address, :tx_count)`
	insertAccountScript = `
	INSERT into accounts (address, height, balance, balance_denomination)
	VALUES (:address, :height, :balance, :balance_denomination)`
	insertNodesScript = `
	INSERT into nodes (address, height, jailed, public_key, service_url, tokens)
	(
		select * from unnest($1::text[], $2::int[], $3::boolean[], $4::text[], $5::text[], $6::numeric[])
	)`
	insertAppsScript = `
	INSERT into apps (address, height, jailed, public_key, staked_tokens)
	(
		select * from unnest($1::text[], $2::int[], $3::boolean[], $4::text[], $5::numeric[])
	)`
	selectTransactionsScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions ORDER BY height DESC;
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`
	selectBlocksScript = `
	DECLARE blocks_cursor CURSOR FOR SELECT * FROM blocks ORDER BY height DESC;
	MOVE absolute %d from blocks_cursor;
	FETCH %d FROM blocks_cursor;
	`
	selectTransactionsByAddressScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions WHERE from_address = '%s' OR to_address = '%s' ORDER BY height DESC;
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`
	selectTransactionByHashScript    = "SELECT * FROM transactions WHERE hash = $1"
	selectBlockByHashScript          = "SELECT * FROM blocks WHERE hash = $1"
	selectTransactionsByHeightScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions WHERE height = '%d';
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`
	selectAccountsByHeightScript = `
	DECLARE accounts_cursor CURSOR FOR SELECT * FROM accounts WHERE height = '%d';
	MOVE absolute %d from accounts_cursor;
	FETCH %d FROM accounts_cursor;
	`
	selectNodesByHeightScript = `
	DECLARE nodes_cursor CURSOR FOR SELECT * FROM nodes WHERE height = '%d';
	MOVE absolute %d from nodes_cursor;
	FETCH %d FROM nodes_cursor;
	`
	selectAppsByHeightScript = `
	DECLARE apps_cursor CURSOR FOR SELECT * FROM apps WHERE height = '%d';
	MOVE absolute %d from apps_cursor;
	FETCH %d FROM apps_cursor;
	`
	selectBlockByHeightScript             = "SELECT * FROM blocks WHERE height = $1"
	selectAccountByAddressAndHeightScript = "SELECT * FROM accounts WHERE address = $1 AND height = $2"
	selectNodeByAddressAndHeightScript    = "SELECT * FROM nodes WHERE address = $1 AND height = $2"
	selectAppByAddressAndHeightScript     = "SELECT * FROM apps WHERE address = $1 AND height = $2"
	selectCountFromTransactions           = "SELECT COUNT(*) FROM transactions"
	selectCountFromBlocks                 = "SELECT COUNT(*) FROM blocks"
	selectCountFromTransactionsByAddress  = "SELECT COUNT(*) FROM transactions WHERE from_address = $1 OR to_address = $1"
	selectCountFromTransactionsByHeight   = "SELECT COUNT(*) FROM transactions WHERE height = $1"
	selectCountFromAccountsByHeight       = "SELECT COUNT(*) FROM accounts WHERE height = $1"
	selectCountFromNodesByHeight          = "SELECT COUNT(*) FROM nodes WHERE height = $1"
	selectCountFromAppsByHeight           = "SELECT COUNT(*) FROM apps WHERE height = $1"
	selectMaxHeightFromBlocks             = "SELECT MAX(height) FROM blocks"

	defaultPerPage = 1000
	defaultPage    = 1

	chainsSeparator = ","
)

var (
	// ErrNoPreviousHeight error when no previous height is stored
	ErrNoPreviousHeight = errors.New("no previous height stored")
	// ErrInvalidAddress error when given address is invalid
	ErrInvalidAddress = errors.New("invalid address")
)

// PostgresDriver struct handler for PostgresDB related functions
type PostgresDriver struct {
	*sqlx.DB
}

// NewPostgresDriverFromConnectionString returns PostgresDriver instance from connection string
func NewPostgresDriverFromConnectionString(connectionString string) (*PostgresDriver, error) {
	db, err := sqlx.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	return &PostgresDriver{
		DB: db,
	}, nil
}

// NewPostgresDriverFromSQLDBInstance returns PostgresDriver instance from sdl.DB instance
// mostly used for mocking tests
func NewPostgresDriverFromSQLDBInstance(db *sql.DB) *PostgresDriver {
	return &PostgresDriver{
		DB: sqlx.NewDb(db, "postgres"),
	}
}

func getPerPageValue(optionsPerPage int) int {
	if optionsPerPage <= 0 {
		return defaultPerPage
	}

	return optionsPerPage
}

func getPageValue(optionsPage int) int {
	if optionsPage <= 0 {
		return defaultPage
	}

	return optionsPage
}

func getMoveValue(perPage, page int) int {
	return (page - 1) * perPage
}

// dbTransaction is struct handler for the transaction with types needed for Postgres processing
type dbTransaction struct {
	ID          int            `db:"id"`
	Hash        string         `db:"hash"`
	FromAddress sql.NullString `db:"from_address"`
	ToAddress   sql.NullString `db:"to_address"`
	AppPubKey   string         `db:"app_pub_key"`
	// It is necessary to save Blockchains as a joined string to be able to use SQL's Unnest with this field
	Blockchains     string    `db:"blockchains"`
	MessageType     string    `db:"message_type"`
	Height          int       `db:"height"`
	Index           int       `db:"index"`
	StdTx           *stdTx    `db:"stdtx"`
	TxResult        *txResult `db:"tx_result"`
	Tx              string    `db:"tx"`
	Entropy         int       `db:"entropy"`
	Fee             int       `db:"fee"`
	FeeDenomination string    `db:"fee_denomination"`
	Amount          int       `db:"amount"`
}

func (t *dbTransaction) toIndexerTransaction() *indexer.Transaction {
	return &indexer.Transaction{
		Hash:            t.Hash,
		FromAddress:     t.FromAddress.String,
		ToAddress:       t.ToAddress.String,
		AppPubKey:       t.AppPubKey,
		Blockchains:     strings.Split(t.Blockchains, chainsSeparator),
		MessageType:     t.MessageType,
		Height:          t.Height,
		Index:           t.Index,
		StdTx:           t.StdTx.StdTx,
		TxResult:        t.TxResult.TxResult,
		Tx:              t.Tx,
		Entropy:         t.Entropy,
		Fee:             t.Fee,
		FeeDenomination: t.FeeDenomination,
		Amount:          t.Amount,
	}
}

func newSQLNullString(value string) sql.NullString {
	if value == "" {
		return sql.NullString{}
	}

	return sql.NullString{
		String: value,
		Valid:  true,
	}
}

func convertIndexerTransactionToDBTransaction(indexerTransaction *indexer.Transaction) *dbTransaction {
	return &dbTransaction{
		Hash:            indexerTransaction.Hash,
		FromAddress:     newSQLNullString(indexerTransaction.FromAddress),
		ToAddress:       newSQLNullString(indexerTransaction.ToAddress),
		AppPubKey:       indexerTransaction.AppPubKey,
		Blockchains:     strings.Join(indexerTransaction.Blockchains, chainsSeparator),
		MessageType:     indexerTransaction.MessageType,
		Height:          indexerTransaction.Height,
		Index:           indexerTransaction.Index,
		StdTx:           &stdTx{StdTx: indexerTransaction.StdTx},
		TxResult:        &txResult{TxResult: indexerTransaction.TxResult},
		Tx:              indexerTransaction.Tx,
		Entropy:         indexerTransaction.Entropy,
		Fee:             indexerTransaction.Fee,
		FeeDenomination: indexerTransaction.FeeDenomination,
		Amount:          indexerTransaction.Amount,
	}
}

// WriteTransactions inserts given transactions to the database
func (d *PostgresDriver) WriteTransactions(txs []*indexer.Transaction) error {
	var hashes, appPubKeys, blockChains, messageTypes, txStrings, feeDenominations []string
	var fromAddresses, toAddresses []sql.NullString
	var heights, indexes, entropies, fees, amounts []int64
	var stdTxs []*stdTx
	var txResults []*txResult

	for _, tx := range txs {
		dbTransaction := convertIndexerTransactionToDBTransaction(tx)
		hashes = append(hashes, dbTransaction.Hash)
		fromAddresses = append(fromAddresses, dbTransaction.FromAddress)
		toAddresses = append(toAddresses, dbTransaction.ToAddress)
		appPubKeys = append(appPubKeys, dbTransaction.AppPubKey)
		blockChains = append(blockChains, dbTransaction.Blockchains)
		messageTypes = append(messageTypes, dbTransaction.MessageType)
		heights = append(heights, int64(dbTransaction.Height))
		indexes = append(indexes, int64(dbTransaction.Index))
		stdTxs = append(stdTxs, dbTransaction.StdTx)
		txResults = append(txResults, dbTransaction.TxResult)
		txStrings = append(txStrings, dbTransaction.Tx)
		entropies = append(entropies, int64(dbTransaction.Entropy))
		fees = append(fees, int64(dbTransaction.Fee))
		feeDenominations = append(feeDenominations, dbTransaction.FeeDenomination)
		amounts = append(amounts, int64(dbTransaction.Amount))
	}

	_, err := d.Exec(insertTransactionsScript,
		pq.StringArray(hashes),
		pq.Array(fromAddresses),
		pq.Array(toAddresses),
		pq.StringArray(appPubKeys),
		pq.StringArray(blockChains),
		pq.StringArray(messageTypes),
		pq.Int64Array(heights),
		pq.Int64Array(indexes),
		pq.Array(stdTxs),
		pq.Array(txResults),
		pq.StringArray(txStrings),
		pq.Int64Array(entropies),
		pq.Int64Array(fees),
		pq.StringArray(feeDenominations),
		pq.Int64Array(amounts))
	if err != nil {
		return err
	}

	return nil
}

// ReadTransactionsOptions optional parameters for ReadTransactions
type ReadTransactionsOptions struct {
	PerPage int
	Page    int
}

// ReadTransactions returns transactions on the database with pagination
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadTransactions(options *ReadTransactionsOptions) ([]*indexer.Transaction, error) {
	perPage := defaultPerPage
	page := defaultPage

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectTransactionsScript, move, perPage)

	var transactions []*dbTransaction

	err = tx.Select(&transactions, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerTransactions []*indexer.Transaction

	for _, dbTransaction := range transactions {
		indexerTransactions = append(indexerTransactions, dbTransaction.toIndexerTransaction())
	}

	return indexerTransactions, nil
}

// ReadTransactionsByAddressOptions optional parameters for ReadTransactionsByAddress
type ReadTransactionsByAddressOptions struct {
	PerPage int
	Page    int
}

// ReadTransactionsByAddress returns transactions with given from address
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadTransactionsByAddress(address string, options *ReadTransactionsByAddressOptions) ([]*indexer.Transaction, error) {
	if !utils.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	perPage := defaultPerPage
	page := defaultPage

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectTransactionsByAddressScript, address, address, move, perPage)

	var transactions []*dbTransaction

	err = tx.Select(&transactions, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerTransactions []*indexer.Transaction

	for _, dbTransaction := range transactions {
		indexerTransactions = append(indexerTransactions, dbTransaction.toIndexerTransaction())
	}

	return indexerTransactions, nil
}

// ReadTransactionsByHeightOptions optional parameters for ReadTransactionsByHeight
type ReadTransactionsByHeightOptions struct {
	PerPage int
	Page    int
}

// ReadTransactionsByHeight returns transactions with given height
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadTransactionsByHeight(height int, options *ReadTransactionsByHeightOptions) ([]*indexer.Transaction, error) {
	perPage := defaultPerPage
	page := defaultPage

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectTransactionsByHeightScript, height, move, perPage)

	var transactions []*dbTransaction

	err = tx.Select(&transactions, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerTransactions []*indexer.Transaction

	for _, dbTransaction := range transactions {
		indexerTransactions = append(indexerTransactions, dbTransaction.toIndexerTransaction())
	}

	return indexerTransactions, nil
}

// ReadTransactionByHash returns transaction in the database with given transaction hash
func (d *PostgresDriver) ReadTransactionByHash(hash string) (*indexer.Transaction, error) {
	var dbTransaction dbTransaction

	err := d.Get(&dbTransaction, selectTransactionByHashScript, hash)
	if err != nil {
		return nil, err
	}

	return dbTransaction.toIndexerTransaction(), nil
}

// GetTransactionsQuantity returns quantity of transactions saved
func (d *PostgresDriver) GetTransactionsQuantity() (int64, error) {
	row := d.QueryRow(selectCountFromTransactions)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}

// GetTransactionsQuantityByAddress returns quantity of transactions with given address saved
func (d *PostgresDriver) GetTransactionsQuantityByAddress(address string) (int64, error) {
	if !utils.ValidateAddress(address) {
		return 0, ErrInvalidAddress
	}

	row := d.QueryRow(selectCountFromTransactionsByAddress, address)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}

// GetTransactionsQuantityByHeight returns quantity of transactions with given height saved
func (d *PostgresDriver) GetTransactionsQuantityByHeight(height int) (int64, error) {
	row := d.QueryRow(selectCountFromTransactionsByHeight, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}

// dbBlock is struct handler for the block with types needed for Postgres processing
type dbBlock struct {
	ID              int       `db:"id"`
	Hash            string    `db:"hash"`
	Height          int       `db:"height"`
	Time            time.Time `db:"time"`
	ProposerAddress string    `db:"proposer_address"`
	TXCount         int       `db:"tx_count"`
}

func (b *dbBlock) toIndexerBlock() *indexer.Block {
	return &indexer.Block{
		Hash:            b.Hash,
		Height:          b.Height,
		Time:            b.Time,
		ProposerAddress: b.ProposerAddress,
		TXCount:         b.TXCount,
	}
}

func convertIndexerBlockToDBBlock(indexerBlock *indexer.Block) *dbBlock {
	return &dbBlock{
		Hash:            indexerBlock.Hash,
		Height:          indexerBlock.Height,
		Time:            indexerBlock.Time,
		ProposerAddress: indexerBlock.ProposerAddress,
		TXCount:         indexerBlock.TXCount,
	}
}

// WriteBlock inserts given block to the database
func (d *PostgresDriver) WriteBlock(block *indexer.Block) error {
	dbBlock := convertIndexerBlockToDBBlock(block)

	_, err := d.NamedExec(insertBlockScript, dbBlock)
	if err != nil {
		return err
	}

	return nil
}

// ReadBlocksOptions optional parameters for ReadBlocks
type ReadBlocksOptions struct {
	PerPage int
	Page    int
}

// ReadBlocks returns all blocks on the database with pagination
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadBlocks(options *ReadBlocksOptions) ([]*indexer.Block, error) {
	perPage := defaultPerPage
	page := defaultPage

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectBlocksScript, move, perPage)

	var blocks []*dbBlock

	err = tx.Select(&blocks, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerBlocks []*indexer.Block

	for _, block := range blocks {
		indexerBlocks = append(indexerBlocks, block.toIndexerBlock())
	}

	return indexerBlocks, nil
}

// ReadBlockByHash returns block in the database with given block hash
func (d *PostgresDriver) ReadBlockByHash(hash string) (*indexer.Block, error) {
	var dbBlock dbBlock

	err := d.Get(&dbBlock, selectBlockByHashScript, hash)
	if err != nil {
		return nil, err
	}

	return dbBlock.toIndexerBlock(), nil
}

// ReadBlockByHeight returns block in the database with given height
func (d *PostgresDriver) ReadBlockByHeight(height int) (*indexer.Block, error) {
	var dbBlock dbBlock

	err := d.Get(&dbBlock, selectBlockByHeightScript, height)
	if err != nil {
		return nil, err
	}

	return dbBlock.toIndexerBlock(), nil
}

// GetMaxHeightInBlocks returns max height saved on blocks' table
func (d *PostgresDriver) GetMaxHeightInBlocks() (int64, error) {
	row := d.QueryRow(selectMaxHeightFromBlocks)

	var maxHeight sql.NullInt64

	err := row.Scan(&maxHeight)
	if err != nil {
		return 0, err
	}

	if !maxHeight.Valid {
		return 0, ErrNoPreviousHeight
	}

	return maxHeight.Int64, nil
}

// GetBlocksQuantity returns quantity of blocks saved
func (d *PostgresDriver) GetBlocksQuantity() (int64, error) {
	row := d.QueryRow(selectCountFromBlocks)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}

// dbAccount is struct handler for the account with types needed for Postgres processing
type dbAccount struct {
	ID                  int    `db:"id"`
	Address             string `db:"address"`
	Height              int    `db:"height"`
	Balance             string `db:"balance"`
	BalanceDenomination string `db:"balance_denomination"`
}

func (a *dbAccount) toIndexerAccount() *indexer.Account {
	balance := new(big.Int)
	balance, _ = balance.SetString(a.Balance, 10)

	return &indexer.Account{
		Address:             a.Address,
		Height:              a.Height,
		Balance:             balance,
		BalanceDenomination: a.BalanceDenomination,
	}
}

func convertIndexerAccountToDBAccount(indexerAccount *indexer.Account) *dbAccount {
	return &dbAccount{
		Address:             indexerAccount.Address,
		Height:              indexerAccount.Height,
		Balance:             indexerAccount.Balance.String(),
		BalanceDenomination: indexerAccount.BalanceDenomination,
	}
}

// WriteAccount inserts given account to the database
func (d *PostgresDriver) WriteAccount(account *indexer.Account) error {
	_, err := d.NamedExec(insertAccountScript, convertIndexerAccountToDBAccount(account))
	if err != nil {
		return err
	}

	return nil
}

// ReadAccountByAddressAndHeight returns an account in the database with given address and height
func (d *PostgresDriver) ReadAccountByAddressAndHeight(address string, height int) (*indexer.Account, error) {
	if !utils.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	var dbAccount dbAccount

	err := d.Get(&dbAccount, selectAccountByAddressAndHeightScript, address, height)
	if err != nil {
		return nil, err
	}

	return dbAccount.toIndexerAccount(), nil
}

// ReadAccountsByHeightOptions optional parameters for ReadAccountsByHeight
type ReadAccountsByHeightOptions struct {
	PerPage int
	Page    int
}

// ReadAccountsByHeight returns accounts with given height
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadAccountsByHeight(height int, options *ReadAccountsByHeightOptions) ([]*indexer.Account, error) {
	perPage := defaultPerPage
	page := defaultPage

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectAccountsByHeightScript, height, move, perPage)

	var accounts []*dbAccount

	err = tx.Select(&accounts, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerAccounts []*indexer.Account

	for _, dbAccount := range accounts {
		indexerAccounts = append(indexerAccounts, dbAccount.toIndexerAccount())
	}

	return indexerAccounts, nil
}

// GetAccountsQuantityByHeight returns quantity of accounts with given height saved
func (d *PostgresDriver) GetAccountsQuantityByHeight(height int) (int64, error) {
	row := d.QueryRow(selectCountFromAccountsByHeight, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}

// dbNode is struct handler for the node with types needed for Postgres processing
type dbNode struct {
	ID         int    `db:"id"`
	Address    string `db:"address"`
	Height     int    `db:"height"`
	Jailed     bool   `db:"jailed"`
	PublicKey  string `db:"public_key"`
	ServiceURL string `db:"service_url"`
	Tokens     string `db:"tokens"`
}

func (n *dbNode) toIndexerNode() *indexer.Node {
	tokens := new(big.Int)
	tokens, _ = tokens.SetString(n.Tokens, 10)

	return &indexer.Node{
		Address:    n.Address,
		Height:     n.Height,
		Jailed:     n.Jailed,
		PublicKey:  n.PublicKey,
		ServiceURL: n.ServiceURL,
		Tokens:     tokens,
	}
}

func convertIndexerNodeToDBNode(indexerNode *indexer.Node) *dbNode {
	return &dbNode{
		Address:    indexerNode.Address,
		Height:     indexerNode.Height,
		Jailed:     indexerNode.Jailed,
		PublicKey:  indexerNode.PublicKey,
		ServiceURL: indexerNode.ServiceURL,
		Tokens:     indexerNode.Tokens.String(),
	}
}

// WriteNodes inserts given nodes to the database
func (d *PostgresDriver) WriteNodes(nodes []*indexer.Node) error {
	var addresses, publicKeys, serviceURLs, allTokens []string
	var heights []int64
	var jaileds []bool

	for _, node := range nodes {
		dbNode := convertIndexerNodeToDBNode(node)
		addresses = append(addresses, dbNode.Address)
		heights = append(heights, int64(dbNode.Height))
		jaileds = append(jaileds, dbNode.Jailed)
		publicKeys = append(publicKeys, dbNode.PublicKey)
		serviceURLs = append(serviceURLs, dbNode.ServiceURL)
		allTokens = append(allTokens, dbNode.Tokens)
	}

	_, err := d.Exec(insertNodesScript, pq.StringArray(addresses),
		pq.Int64Array(heights),
		pq.BoolArray(jaileds),
		pq.StringArray(publicKeys),
		pq.StringArray(serviceURLs),
		pq.StringArray(allTokens))
	if err != nil {
		return err
	}

	return nil
}

// ReadNodeByAddressAndHeight returns a node in the database with given address and height
func (d *PostgresDriver) ReadNodeByAddressAndHeight(address string, height int) (*indexer.Node, error) {
	if !utils.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	var dbNode dbNode

	err := d.Get(&dbNode, selectNodeByAddressAndHeightScript, address, height)
	if err != nil {
		return nil, err
	}

	return dbNode.toIndexerNode(), nil
}

// ReadNodesByHeightOptions optional parameters for ReadNodesByHeight
type ReadNodesByHeightOptions struct {
	PerPage int
	Page    int
}

// ReadNodesByHeight returns nodes with given height
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadNodesByHeight(height int, options *ReadNodesByHeightOptions) ([]*indexer.Node, error) {
	perPage := defaultPerPage
	page := defaultPage

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectNodesByHeightScript, height, move, perPage)

	var nodes []*dbNode

	err = tx.Select(&nodes, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerNodes []*indexer.Node

	for _, dbNode := range nodes {
		indexerNodes = append(indexerNodes, dbNode.toIndexerNode())
	}

	return indexerNodes, nil
}

// GetNodesQuantityByHeight returns quantity of nodes with given height saved
func (d *PostgresDriver) GetNodesQuantityByHeight(height int) (int64, error) {
	row := d.QueryRow(selectCountFromNodesByHeight, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}

// dbApp is struct handler for the app with types needed for Postgres processing
type dbApp struct {
	ID           int    `db:"id"`
	Address      string `db:"address"`
	Height       int    `db:"height"`
	Jailed       bool   `db:"jailed"`
	PublicKey    string `db:"public_key"`
	StakedTokens string `db:"staked_tokens"`
}

func (a *dbApp) toIndexerApp() *indexer.App {
	stakedTokens := new(big.Int)
	stakedTokens, _ = stakedTokens.SetString(a.StakedTokens, 10)

	return &indexer.App{
		Address:      a.Address,
		Height:       a.Height,
		Jailed:       a.Jailed,
		PublicKey:    a.PublicKey,
		StakedTokens: stakedTokens,
	}
}

func convertIndexerAppToDBApp(indexerApp *indexer.App) *dbApp {
	return &dbApp{
		Address:      indexerApp.Address,
		Height:       indexerApp.Height,
		Jailed:       indexerApp.Jailed,
		PublicKey:    indexerApp.PublicKey,
		StakedTokens: indexerApp.StakedTokens.String(),
	}
}

// WriteApps inserts given apps to the database
func (d *PostgresDriver) WriteApps(apps []*indexer.App) error {
	var addresses, publicKeys, allStakedTokens []string
	var heights []int64
	var jaileds []bool

	for _, app := range apps {
		dbApp := convertIndexerAppToDBApp(app)
		addresses = append(addresses, dbApp.Address)
		heights = append(heights, int64(dbApp.Height))
		jaileds = append(jaileds, dbApp.Jailed)
		publicKeys = append(publicKeys, dbApp.PublicKey)
		allStakedTokens = append(allStakedTokens, dbApp.StakedTokens)
	}

	_, err := d.Exec(insertAppsScript, pq.StringArray(addresses),
		pq.Int64Array(heights),
		pq.BoolArray(jaileds),
		pq.StringArray(publicKeys),
		pq.StringArray(allStakedTokens))
	if err != nil {
		return err
	}

	return nil
}

// ReadAppByAddressAndHeight returns an app in the database with given address and height
func (d *PostgresDriver) ReadAppByAddressAndHeight(address string, height int) (*indexer.App, error) {
	if !utils.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	var dbApp dbApp

	err := d.Get(&dbApp, selectAppByAddressAndHeightScript, address, height)
	if err != nil {
		return nil, err
	}

	return dbApp.toIndexerApp(), nil
}

// ReadAppsByHeightOptions optional parameters for ReadAppsByHeight
type ReadAppsByHeightOptions struct {
	PerPage int
	Page    int
}

// ReadAppsByHeight returns apps with given height
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadAppsByHeight(height int, options *ReadAppsByHeightOptions) ([]*indexer.App, error) {
	perPage := defaultPerPage
	page := defaultPage

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectAppsByHeightScript, height, move, perPage)

	var apps []*dbApp

	err = tx.Select(&apps, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerApps []*indexer.App

	for _, dbApp := range apps {
		indexerApps = append(indexerApps, dbApp.toIndexerApp())
	}

	return indexerApps, nil
}

// GetAppsQuantityByHeight returns quantity of apps with given height saved
func (d *PostgresDriver) GetAppsQuantityByHeight(height int) (int64, error) {
	row := d.QueryRow(selectCountFromAppsByHeight, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}
