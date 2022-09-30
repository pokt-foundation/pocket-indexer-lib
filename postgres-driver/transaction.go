package postgresdriver

import (
	"database/sql"
	"fmt"
	"math/big"
	"strings"

	"github.com/lib/pq"
	"github.com/pokt-foundation/pocket-go/utils"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

const (
	insertTransactionsScript = `
	INSERT into transactions (hash, from_address, to_address, app_pub_key, blockchains, message_type, height, index, stdtx, tx_result, tx, entropy, fee, fee_denomination, amount)
	(
		select * from unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::int[], $8::int[], $9::jsonb[], $10::jsonb[], $11::text[], $12::numeric[], $13::int[], $14::text[], $15::numeric[])
	)`
	selectTransactionsScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions ORDER BY height %s;
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`

	selectTransactionsByAddressScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions WHERE from_address = '%s' OR to_address = '%s' ORDER BY height DESC;
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`
	selectTransactionByHashScript    = "SELECT * FROM transactions WHERE hash = $1"
	selectTransactionsByHeightScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions WHERE height = '%d';
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`
	selectTransactionsByMaxHeightScript = `
	DECLARE transactions_cursor CURSOR FOR SELECT * FROM transactions WHERE height = (SELECT MAX(height) FROM transactions);
	MOVE absolute %d from transactions_cursor;
	FETCH %d FROM transactions_cursor;
	`
	selectCountFromTransactions            = "SELECT COUNT(*) FROM transactions"
	selectCountFromTransactionsByAddress   = "SELECT COUNT(*) FROM transactions WHERE from_address = $1 OR to_address = $1"
	selectCountFromTransactionsByHeight    = "SELECT COUNT(*) FROM transactions WHERE height = $1"
	selectCountFromTransactionsByMaxHeight = "SELECT COUNT(*) FROM transactions WHERE height = (SELECT MAX(height) FROM transactions)"

	chainsSeparator = ","
)

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
	Amount          string    `db:"amount"`
}

func (t *dbTransaction) toIndexerTransaction() *types.Transaction {
	amount := new(big.Int)
	amount, _ = amount.SetString(t.Amount, 10)

	return &types.Transaction{
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
		Amount:          amount,
	}
}

func convertIndexerTransactionToDBTransaction(indexerTransaction *types.Transaction) *dbTransaction {
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
		Amount:          indexerTransaction.Amount.String(),
	}
}

// WriteTransactions inserts given transactions to the database
func (d *PostgresDriver) WriteTransactions(txs []*types.Transaction) error {
	var hashes, appPubKeys, blockChains, messageTypes, txStrings, feeDenominations, amounts []string
	var fromAddresses, toAddresses []sql.NullString
	var heights, indexes, entropies, fees []int64
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
		amounts = append(amounts, dbTransaction.Amount)
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
		pq.StringArray(amounts))
	if err != nil {
		return err
	}

	return nil
}

// ReadTransactions returns transactions on the database with pagination
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadTransactions(options *types.ReadTransactionsOptions) ([]*types.Transaction, error) {
	perPage := defaultPerPage
	page := defaultPage
	order := defaultOrder

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
		order = getOrderValue(options.Order)
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(selectTransactionsScript, order, move, perPage)

	var transactions []*dbTransaction

	err = tx.Select(&transactions, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerTransactions []*types.Transaction

	for _, dbTransaction := range transactions {
		indexerTransactions = append(indexerTransactions, dbTransaction.toIndexerTransaction())
	}

	return indexerTransactions, nil
}

// ReadTransactionsByAddress returns transactions with given from address
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadTransactionsByAddress(address string, options *types.ReadTransactionsByAddressOptions) ([]*types.Transaction, error) {
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

	var indexerTransactions []*types.Transaction

	for _, dbTransaction := range transactions {
		indexerTransactions = append(indexerTransactions, dbTransaction.toIndexerTransaction())
	}

	return indexerTransactions, nil
}

// ReadTransactionsByHeight returns transactions with given height
// height 0 is last height
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadTransactionsByHeight(height int, options *types.ReadTransactionsByHeightOptions) ([]*types.Transaction, error) {
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

	query := getHeightOptionalQuery(selectTransactionsByHeightScript, selectTransactionsByMaxHeightScript,
		height, move, perPage)

	var transactions []*dbTransaction

	err = tx.Select(&transactions, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerTransactions []*types.Transaction

	for _, dbTransaction := range transactions {
		indexerTransactions = append(indexerTransactions, dbTransaction.toIndexerTransaction())
	}

	return indexerTransactions, nil
}

// ReadTransactionByHash returns transaction in the database with given transaction hash
func (d *PostgresDriver) ReadTransactionByHash(hash string) (*types.Transaction, error) {
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
// height 0 is last height
func (d *PostgresDriver) GetTransactionsQuantityByHeight(height int) (int64, error) {
	row := d.getRowWithOptionalHeight(selectCountFromTransactionsByHeight,
		selectCountFromTransactionsByMaxHeight, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}
