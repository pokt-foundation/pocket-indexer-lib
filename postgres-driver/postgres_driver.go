package postgresdriver

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
)

const (
	insertTransactionsScript = `
	INSERT into transactions (hash, from_address, to_address, app_pub_key, blockchains, message_type, height, index, proof, stdtx, tx_result, tx, entropy, fee, fee_denomination)
	VALUES (:hash, :from_address, :to_address, :app_pub_key, :blockchains, :message_type, :height, :index, :proof, :stdtx, :tx_result, :tx, :entropy, :fee, :fee_denomination)`
	selectAllTransactionsScript   = "SELECT * FROM transactions"
	selectTransactionByHashScript = "SELECT * FROM transactions WHERE hash = $1"
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

type transaction struct {
	ID              int            `db:"id"`
	Hash            string         `db:"hash"`
	FromAddress     string         `db:"from_address"`
	ToAddress       string         `db:"to_address"`
	AppPubKey       string         `db:"app_pub_key"`
	Blockchains     pq.StringArray `db:"blockchains"`
	MessageType     string         `db:"message_type"`
	Height          int            `db:"height"`
	Index           int            `db:"index"`
	Proof           *proof         `db:"proof"`
	StdTx           *stdTx         `db:"stdtx"`
	TxResult        *txResult      `db:"tx_result"`
	Tx              string         `db:"tx"`
	Entropy         int            `db:"entropy"`
	Fee             int            `db:"fee"`
	FeeDenomination string         `db:"fee_denomination"`
}

func (t *transaction) toIndexerTransaction() *indexer.Transaction {
	return &indexer.Transaction{
		Hash:            t.Hash,
		FromAddress:     t.FromAddress,
		ToAddress:       t.ToAddress,
		AppPubKey:       t.AppPubKey,
		Blockchains:     t.Blockchains,
		MessageType:     t.MessageType,
		Height:          t.Height,
		Index:           t.Index,
		Proof:           t.Proof.TransactionProof,
		StdTx:           t.StdTx.StdTx,
		TxResult:        t.TxResult.TxResult,
		Tx:              t.Tx,
		Entropy:         t.Entropy,
		Fee:             t.Fee,
		FeeDenomination: t.FeeDenomination,
	}
}

func convertIndexerTransactionToTransaction(indexerTransaction *indexer.Transaction) *transaction {
	return &transaction{
		Hash:            indexerTransaction.Hash,
		FromAddress:     indexerTransaction.FromAddress,
		ToAddress:       indexerTransaction.ToAddress,
		AppPubKey:       indexerTransaction.AppPubKey,
		Blockchains:     indexerTransaction.Blockchains,
		MessageType:     indexerTransaction.MessageType,
		Height:          indexerTransaction.Height,
		Index:           indexerTransaction.Index,
		Proof:           &proof{TransactionProof: indexerTransaction.Proof},
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
	var transactions []*transaction

	for _, tx := range txs {
		transactions = append(transactions, convertIndexerTransactionToTransaction(tx))
	}

	_, err := d.NamedExec(insertTransactionsScript, transactions)
	if err != nil {
		return err
	}

	return nil
}

// ReadTransactions returns all transactions on the database
// TODO: add pagination
func (d *PostgresDriver) ReadTransactions() ([]*indexer.Transaction, error) {
	var transactions []*transaction

	err := d.Select(&transactions, selectAllTransactionsScript)
	if err != nil {
		return nil, err
	}

	var indexerTransactions []*indexer.Transaction

	for _, transaction := range transactions {
		indexerTransactions = append(indexerTransactions, transaction.toIndexerTransaction())
	}

	return indexerTransactions, nil
}

// ReadTransaction returns transaction in the database with given transaction hash
func (d *PostgresDriver) ReadTransaction(hash string) (*indexer.Transaction, error) {
	var transaction transaction

	err := d.Get(&transaction, selectTransactionByHashScript, hash)
	if err != nil {
		return nil, err
	}

	return transaction.toIndexerTransaction(), nil
}

// TODO: implement WriteBlock func
