package postgresdriver

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
)

const (
	insertTransactionsScript = `
	INSERT into transactions (hash, from_address, to_address, app_pub_key, blockchains, message_type, height, index, stdtx, tx_result, tx, entropy, fee, fee_denomination)
	VALUES (:hash, :from_address, :to_address, :app_pub_key, :blockchains, :message_type, :height, :index, :stdtx, :tx_result, :tx, :entropy, :fee, :fee_denomination)`
	insertBlockScript = `
	INSERT into blocks (hash, height, time, proposer_address, tx_count, relay_count)
	VALUES (:hash, :height, :time, :proposer_address, :tx_count, :relay_count)`
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
	selectTransactionsByFromAddressScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions WHERE from_address = '%s' ORDER BY height DESC;
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`
	selectTransactionByHashScript = "SELECT * FROM transactions WHERE hash = $1"
	selectBlockByHashScript       = "SELECT * FROM blocks WHERE hash = $1"
	selectMaxHeightFromBlocks     = "SELECT MAX(height) FROM blocks"

	defaultPerPage = 1000
	defaultPage    = 1
	addressLength  = 40
)

var (
	// ErrNoPreviousHeight error when no previous height is stored
	ErrNoPreviousHeight = errors.New("no previous height stored")
	// ErrInvalidFromAddress error when given from address is invalid
	ErrInvalidFromAddress = errors.New("invalid from address")

	addressRegex = regexp.MustCompile("^[a-f0-9]+$")
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
	if page == 1 {
		return 0
	}

	return (page - 1) * perPage
}

// dbTransaction is struct handler for the transaction with types needed for Postgres processing
type dbTransaction struct {
	ID              int            `db:"id"`
	Hash            string         `db:"hash"`
	FromAddress     string         `db:"from_address"`
	ToAddress       string         `db:"to_address"`
	AppPubKey       string         `db:"app_pub_key"`
	Blockchains     pq.StringArray `db:"blockchains"`
	MessageType     string         `db:"message_type"`
	Height          int            `db:"height"`
	Index           int            `db:"index"`
	StdTx           *stdTx         `db:"stdtx"`
	TxResult        *txResult      `db:"tx_result"`
	Tx              string         `db:"tx"`
	Entropy         int            `db:"entropy"`
	Fee             int            `db:"fee"`
	FeeDenomination string         `db:"fee_denomination"`
}

func (t *dbTransaction) toIndexerTransaction() *indexer.Transaction {
	return &indexer.Transaction{
		Hash:            t.Hash,
		FromAddress:     t.FromAddress,
		ToAddress:       t.ToAddress,
		AppPubKey:       t.AppPubKey,
		Blockchains:     t.Blockchains,
		MessageType:     t.MessageType,
		Height:          t.Height,
		Index:           t.Index,
		StdTx:           t.StdTx.StdTx,
		TxResult:        t.TxResult.TxResult,
		Tx:              t.Tx,
		Entropy:         t.Entropy,
		Fee:             t.Fee,
		FeeDenomination: t.FeeDenomination,
	}
}

func convertIndexerTransactionToDBTransaction(indexerTransaction *indexer.Transaction) *dbTransaction {
	return &dbTransaction{
		Hash:            indexerTransaction.Hash,
		FromAddress:     indexerTransaction.FromAddress,
		ToAddress:       indexerTransaction.ToAddress,
		AppPubKey:       indexerTransaction.AppPubKey,
		Blockchains:     indexerTransaction.Blockchains,
		MessageType:     indexerTransaction.MessageType,
		Height:          indexerTransaction.Height,
		Index:           indexerTransaction.Index,
		StdTx:           &stdTx{StdTx: indexerTransaction.StdTx},
		TxResult:        &txResult{TxResult: indexerTransaction.TxResult},
		Tx:              indexerTransaction.Tx,
		Entropy:         indexerTransaction.Entropy,
		Fee:             indexerTransaction.Fee,
		FeeDenomination: indexerTransaction.FeeDenomination,
	}
}

// WriteTransactions inserts given transactions to the database
func (d *PostgresDriver) WriteTransactions(txs []*indexer.Transaction) error {
	var transactions []*dbTransaction

	for _, tx := range txs {
		transactions = append(transactions, convertIndexerTransactionToDBTransaction(tx))
	}

	_, err := d.NamedExec(insertTransactionsScript, transactions)
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
// Optinal values defaults: page: 1, perPage: 1000
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

// ReadTransactionsByFromAddressOptions optional parameters for ReadTransactionsByFromAddress
type ReadTransactionsByFromAddressOptions struct {
	PerPage int
	Page    int
}

func validateAddress(fromAddress string) bool {
	return addressRegex.MatchString(fromAddress) && len(fromAddress) == addressLength
}

// ReadTransactionsByFromAddress returns transactions with given from address
// Optinal values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadTransactionsByFromAddress(fromAddress string, options *ReadTransactionsOptions) ([]*indexer.Transaction, error) {
	if !validateAddress(fromAddress) {
		return nil, ErrInvalidFromAddress
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

	query := fmt.Sprintf(selectTransactionsByFromAddressScript, fromAddress, move, perPage)

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

// ReadTransaction returns transaction in the database with given transaction hash
func (d *PostgresDriver) ReadTransaction(hash string) (*indexer.Transaction, error) {
	var dbTransaction dbTransaction

	err := d.Get(&dbTransaction, selectTransactionByHashScript, hash)
	if err != nil {
		return nil, err
	}

	return dbTransaction.toIndexerTransaction(), nil
}

// dbBlock is struct handler for the block with types needed for Postgres processing
type dbBlock struct {
	ID              int       `db:"id"`
	Hash            string    `db:"hash"`
	Height          int       `db:"height"`
	Time            time.Time `db:"time"`
	ProposerAddress string    `db:"proposer_address"`
	TXCount         int       `db:"tx_count"`
	RelayCount      int       `db:"relay_count"`
}

func (b *dbBlock) toIndexerBlock() *indexer.Block {
	return &indexer.Block{
		Hash:            b.Hash,
		Height:          b.Height,
		Time:            b.Time,
		ProposerAddress: b.ProposerAddress,
		TXCount:         b.TXCount,
		RelayCount:      b.RelayCount,
	}
}

func convertIndexerBlockToDBBlock(indexerBlock *indexer.Block) *dbBlock {
	return &dbBlock{
		Hash:            indexerBlock.Hash,
		Height:          indexerBlock.Height,
		Time:            indexerBlock.Time,
		ProposerAddress: indexerBlock.ProposerAddress,
		TXCount:         indexerBlock.TXCount,
		RelayCount:      indexerBlock.RelayCount,
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
// Optinal values defaults: page: 1, perPage: 1000
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

// ReadBlock returns block in the database with given block hash
func (d *PostgresDriver) ReadBlock(hash string) (*indexer.Block, error) {
	var dbBlock dbBlock

	err := d.Get(&dbBlock, selectBlockByHashScript, hash)
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
