package postgresdriver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/pokt-foundation/pocket-go/provider"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
	"github.com/stretchr/testify/require"
)

func TestPostgresDriver_WriteTransactions(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	testProvStdTx := &provider.StdTx{
		Entropy: 3223323,
		Fee: []*provider.Fee{
			{
				Amount: "10000",
				Denom:  "upokt",
			},
		},
		Msg: &provider.TxMsg{
			Type: "pos/Send",
			Value: map[string]any{
				"from_address": "addssd",
				"amount":       "462000000",
				"chains": []any{
					"0021",
				},
			},
		},
		Signature: &provider.TxSignature{
			PubKey: "adasdsfd",
		},
	}

	testStdTx := &stdTx{
		StdTx: testProvStdTx,
	}

	encodedTestStdTx, err := testStdTx.Value()
	c.NoError(err)

	mock.ExpectExec("INSERT into transactions").WithArgs(pq.StringArray([]string{"AF5BB3EAFF431E2E5E784D639825979FF20A779725BFE61D4521340F70C3996D0"}),
		pq.StringArray([]string{"addssd"}), pq.Array([]sql.NullString{{}}), pq.StringArray([]string{"adasdsfd"}), pq.StringArray([]string{"0021"}),
		pq.StringArray([]string{"pos/Send"}), pq.Int64Array([]int64{0}), pq.Int64Array([]int64{0}), pq.Array([]driver.Value{encodedTestStdTx}),
		pq.Array([]driver.Value{"{}"}), pq.StringArray([]string{""}), pq.Int64Array([]int64{3223323}), pq.Int64Array([]int64{10000}),
		pq.StringArray([]string{"upokt"}), pq.StringArray([]string{"462000000"})).
		WillReturnResult(sqlmock.NewResult(1, 1))

	mock.ExpectExec("INSERT into transactions").WithArgs(pq.StringArray([]string{"AF5BB3EAFF431E2E5E784D639825979FF20A779725BFE61D4521340F70C3996D0"}),
		pq.StringArray([]string{"addssd"}), pq.Array([]sql.NullString{{}}), pq.StringArray([]string{"adasdsfd"}), pq.StringArray([]string{"0021"}),
		pq.StringArray([]string{"pos/Send"}), pq.Int64Array([]int64{0}), pq.Int64Array([]int64{0}), pq.Array([]driver.Value{encodedTestStdTx}),
		pq.Array([]driver.Value{"{}"}), pq.StringArray([]string{""}), pq.Int64Array([]int64{3223323}), pq.Int64Array([]int64{10000}),
		pq.StringArray([]string{"upokt"}), pq.StringArray([]string{"462000000"})).
		WillReturnError(errors.New("dummy error"))

	driver := NewPostgresDriverFromSQLDBInstance(db)

	transactionToSend := []*indexer.Transaction{
		{
			Hash:            "AF5BB3EAFF431E2E5E784D639825979FF20A779725BFE61D4521340F70C3996D0",
			FromAddress:     "addssd",
			ToAddress:       "",
			AppPubKey:       "adasdsfd",
			Blockchains:     []string{"0021"},
			MessageType:     "pos/Send",
			Entropy:         3223323,
			Fee:             10000,
			FeeDenomination: "upokt",
			Amount:          big.NewInt(462000000),
			StdTx:           testProvStdTx,
		},
	}

	err = driver.WriteTransactions(transactionToSend)
	c.NoError(err)

	err = driver.WriteTransactions(transactionToSend)
	c.EqualError(err, "dummy error")
}

func TestPostgresDriver_ReadTransactions(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	testStdTx := &stdTx{
		StdTx: &provider.StdTx{},
	}

	encodedTestStdTx, err := testStdTx.Value()
	c.NoError(err)

	testTxResult := &txResult{
		TxResult: &provider.TxResult{},
	}

	encodedTxResult, err := testTxResult.Value()
	c.NoError(err)

	rows := sqlmock.NewRows([]string{"id", "hash", "from_address", "to_address", "stdtx", "tx_result"}).
		AddRow(1, "ABCD", "abcd", "dbcv", encodedTestStdTx, encodedTxResult).
		AddRow(2, "ABFD", "abfd", "fbcv", encodedTestStdTx, encodedTxResult)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	transactions, err := driver.ReadTransactions(&ReadTransactionsOptions{Page: 2, PerPage: 3})
	c.NoError(err)
	c.Len(transactions, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	transactions, err = driver.ReadTransactions(nil)
	c.EqualError(err, "dummy error")
	c.Empty(transactions)
}

func TestPostgresDriver_ReadTransactionsByAddress(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	testStdTx := &stdTx{
		StdTx: &provider.StdTx{},
	}

	encodedTestStdTx, err := testStdTx.Value()
	c.NoError(err)

	testTxResult := &txResult{
		TxResult: &provider.TxResult{},
	}

	encodedTxResult, err := testTxResult.Value()
	c.NoError(err)

	rows := sqlmock.NewRows([]string{"id", "hash", "from_address", "to_address", "stdtx", "tx_result"}).
		AddRow(1, "ABCD", "1f32488b1db60fe528ab21e3cc26c96696be3faa", "dbcv", encodedTestStdTx, encodedTxResult).
		AddRow(2, "ABFD", "1f32488b1db60fe528ab21e3cc26c96696be3faa", "fbcv", encodedTestStdTx, encodedTxResult)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	transactions, err := driver.ReadTransactionsByAddress(";DROP DATABASE;", &ReadTransactionsByAddressOptions{Page: 2, PerPage: 3})
	c.Equal(ErrInvalidAddress, err)
	c.Empty(transactions)

	transactions, err = driver.ReadTransactionsByAddress("1f32488b1db60fe528ab21e3cc26c96696be3faa", &ReadTransactionsByAddressOptions{Page: 2, PerPage: 3})
	c.NoError(err)
	c.Len(transactions, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	transactions, err = driver.ReadTransactionsByAddress("1f32488b1db60fe528ab21e3cc26c96696be3faa", nil)
	c.EqualError(err, "dummy error")
	c.Empty(transactions)
}

func TestPostgresDriver_ReadTransactionsByHeight(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	testStdTx := &stdTx{
		StdTx: &provider.StdTx{},
	}

	encodedTestStdTx, err := testStdTx.Value()
	c.NoError(err)

	testTxResult := &txResult{
		TxResult: &provider.TxResult{},
	}

	encodedTxResult, err := testTxResult.Value()
	c.NoError(err)

	rows := sqlmock.NewRows([]string{"id", "hash", "from_address", "to_address", "height", "stdtx", "tx_result"}).
		AddRow(1, "ABCD", "1f32488b1db60fe528ab21e3cc26c96696be3faa", "dbcv", 21, encodedTestStdTx, encodedTxResult).
		AddRow(2, "ABFD", "1f32488b1db60fe528ab21e3cc26c96696be3faa", "fbcv", 21, encodedTestStdTx, encodedTxResult)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	transactions, err := driver.ReadTransactionsByHeight(21, &ReadTransactionsByHeightOptions{Page: 2, PerPage: 3})
	c.NoError(err)
	c.Len(transactions, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	transactions, err = driver.ReadTransactionsByHeight(21, nil)
	c.EqualError(err, "dummy error")
	c.Empty(transactions)
}

func TestPostgresDriver_ReadTransaction(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	testStdTx := &stdTx{
		StdTx: &provider.StdTx{},
	}

	encodedTestStdTx, err := testStdTx.Value()
	c.NoError(err)

	testTxResult := &txResult{
		TxResult: &provider.TxResult{},
	}

	encodedTxResult, err := testTxResult.Value()
	c.NoError(err)

	rows := sqlmock.NewRows([]string{"id", "hash", "from_address", "to_address", "stdtx", "tx_result"}).
		AddRow(1, "ABCD", "abcd", "dbcv", encodedTestStdTx, encodedTxResult)

	mock.ExpectQuery("^SELECT (.+) FROM transactions (.+)").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	transaction, err := driver.ReadTransactionByHash("ABCD")
	c.NoError(err)
	c.NotEmpty(transaction)

	mock.ExpectQuery("^SELECT (.+) FROM transactions (.+)").WillReturnError(errors.New("dummy error"))

	transaction, err = driver.ReadTransactionByHash("ABCD")
	c.EqualError(err, "dummy error")
	c.Empty(transaction)
}

func TestPostgresDriver_GetTransactionsQuantity(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM transactions").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetTransactionsQuantity()
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM transactions").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetTransactionsQuantity()
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}

func TestPostgresDriver_GetTransactionsQuantityByAddress(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM transactions").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetTransactionsQuantityByAddress("1f32488b1db60fe528ab21e3cc26c96696be3faa")
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	maxHeight, err = driver.GetTransactionsQuantityByAddress("1f32488b1db60fe528ab21e3cc26c96696be3fa")
	c.Equal(ErrInvalidAddress, err)
	c.Empty(maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM transactions").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetTransactionsQuantityByAddress("1f32488b1db60fe528ab21e3cc26c96696be3faa")
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}

func TestPostgresDriver_GetTransactionsQuantityByHeight(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM transactions").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetTransactionsQuantityByHeight(21)
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM transactions").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetTransactionsQuantityByHeight(21)
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}

func TestPostgresDriver_WriteBlock(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	mock.ExpectExec("INSERT into blocks").WithArgs("AF5BB3EAFF431E2E5E784D639825979FF20A779725BFE61D4521340F70C3996D0",
		21, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "A2143929B30CBC3E7A30C2DE06B385BCF874134B", 32).
		WillReturnResult(sqlmock.NewResult(1, 1))

	driver := NewPostgresDriverFromSQLDBInstance(db)

	err = driver.WriteBlock(&indexer.Block{
		Hash:            "AF5BB3EAFF431E2E5E784D639825979FF20A779725BFE61D4521340F70C3996D0",
		Height:          21,
		Time:            time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local),
		ProposerAddress: "A2143929B30CBC3E7A30C2DE06B385BCF874134B",
		TXCount:         32,
	})
	c.NoError(err)

	mock.ExpectExec("INSERT into blocks").WithArgs("AF5BB3EAFF431E2E5E784D639825979FF20A779725BFE61D4521340F70C3996D0",
		21, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "A2143929B30CBC3E7A30C2DE06B385BCF874134B", 32).
		WillReturnError(errors.New("dummy error"))

	err = driver.WriteBlock(&indexer.Block{
		Hash:            "AF5BB3EAFF431E2E5E784D639825979FF20A779725BFE61D4521340F70C3996D0",
		Height:          21,
		Time:            time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local),
		ProposerAddress: "A2143929B30CBC3E7A30C2DE06B385BCF874134B",
		TXCount:         32,
	})
	c.EqualError(err, "dummy error")
}

func TestPostgresDriver_ReadBlocks(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "hash", "height", "time", "proposer_address", "tx_count"}).
		AddRow(1, "ABCD", 21, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "ABCD", 21).
		AddRow(2, "ABCD", 21, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "ABCD", 21)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	blocks, err := driver.ReadBlocks(&ReadBlocksOptions{Page: 21, PerPage: 7})
	c.NoError(err)
	c.Len(blocks, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	blocks, err = driver.ReadBlocks(&ReadBlocksOptions{})
	c.EqualError(err, "dummy error")
	c.Empty(blocks)
}

func TestPostgresDriver_ReadBlockByHash(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "hash", "height", "time", "proposer_address", "tx_count"}).
		AddRow(1, "ABCD", 21, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "ABCD", 21)

	mock.ExpectQuery("^SELECT (.+) FROM blocks (.+)").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	block, err := driver.ReadBlockByHash("ABCD")
	c.NoError(err)
	c.NotEmpty(block)

	mock.ExpectQuery("^SELECT (.+) FROM blocks (.+)").WillReturnError(errors.New("dummy error"))

	block, err = driver.ReadBlockByHash("ABCD")
	c.EqualError(err, "dummy error")
	c.Empty(block)
}

func TestPostgresDriver_ReadBlockByHeight(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "hash", "height", "time", "proposer_address", "tx_count"}).
		AddRow(1, "ABCD", 21, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "ABCD", 21)

	mock.ExpectQuery("^SELECT (.+) FROM blocks (.+)").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	block, err := driver.ReadBlockByHeight(21)
	c.NoError(err)
	c.NotEmpty(block)

	rows = sqlmock.NewRows([]string{"id", "hash", "height", "time", "proposer_address", "tx_count"}).
		AddRow(1, "ABCD", 21, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "ABCD", 21)

	mock.ExpectQuery("^SELECT (.+) FROM blocks (.+)").WillReturnRows(rows)

	block, err = driver.ReadBlockByHeight(0)
	c.NoError(err)
	c.NotEmpty(block)

	mock.ExpectQuery("^SELECT (.+) FROM blocks (.+)").WillReturnError(errors.New("dummy error"))

	block, err = driver.ReadBlockByHeight(21)
	c.EqualError(err, "dummy error")
	c.Empty(block)

	mock.ExpectQuery("^SELECT (.+) FROM blocks (.+)").WillReturnError(errors.New("dummy error"))

	block, err = driver.ReadBlockByHeight(0)
	c.EqualError(err, "dummy error")
	c.Empty(block)
}

func TestPostgresDriver_GetMaxHeightInBlocks(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"max"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM blocks").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetMaxHeightInBlocks()
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	rows = sqlmock.NewRows([]string{"max"}).AddRow(nil)

	mock.ExpectQuery("^SELECT (.+) FROM blocks").WillReturnRows(rows)

	maxHeight, err = driver.GetMaxHeightInBlocks()
	c.Equal(ErrNoPreviousHeight, err)
	c.Empty(maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM blocks").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetMaxHeightInBlocks()
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}

func TestPostgresDriver_GetBlocksQuantity(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM blocks").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetBlocksQuantity()
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM blocks").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetBlocksQuantity()
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}

func TestPostgresDriver_WriteAccount(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	mock.ExpectExec("INSERT into accounts").WithArgs("00353abd21ef72725b295ba5a9a5eb6082548e21",
		21, "212121", "upokt").
		WillReturnResult(sqlmock.NewResult(1, 1))

	driver := NewPostgresDriverFromSQLDBInstance(db)

	err = driver.WriteAccount(&indexer.Account{
		Address:             "00353abd21ef72725b295ba5a9a5eb6082548e21",
		Height:              21,
		Balance:             big.NewInt(212121),
		BalanceDenomination: "upokt",
	})
	c.NoError(err)

	mock.ExpectExec("INSERT into accounts").WithArgs("00353abd21ef72725b295ba5a9a5eb6082548e21",
		21, "212121", "upokt").
		WillReturnError(errors.New("dummy error"))

	err = driver.WriteAccount(&indexer.Account{
		Address:             "00353abd21ef72725b295ba5a9a5eb6082548e21",
		Height:              21,
		Balance:             big.NewInt(212121),
		BalanceDenomination: "upokt",
	})
	c.EqualError(err, "dummy error")
}

func TestPostgresDriver_ReadAccountByAddress(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "address", "height", "balance", "balance_denomination"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, "212121", "upokt")

	mock.ExpectQuery("^SELECT (.+) FROM accounts (.+)").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	account, err := driver.ReadAccountByAddress("00353abd21ef72725b295ba5a9a5eb6082548e2", &ReadAccountByAddressOptions{Height: 21})
	c.Equal(ErrInvalidAddress, err)
	c.Empty(account)

	account, err = driver.ReadAccountByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &ReadAccountByAddressOptions{Height: 21})
	c.NoError(err)
	c.NotEmpty(account)

	rows = sqlmock.NewRows([]string{"id", "address", "height", "balance", "balance_denomination"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, "212121", "upokt")

	mock.ExpectQuery("^SELECT (.+) FROM accounts (.+)").WillReturnRows(rows)

	account, err = driver.ReadAccountByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", nil)
	c.NoError(err)
	c.NotEmpty(account)

	mock.ExpectQuery("^SELECT (.+) FROM accounts (.+)").WillReturnError(errors.New("dummy error"))

	account, err = driver.ReadAccountByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &ReadAccountByAddressOptions{Height: 21})
	c.EqualError(err, "dummy error")
	c.Empty(account)

	mock.ExpectQuery("^SELECT (.+) FROM accounts (.+)").WillReturnError(errors.New("dummy error"))

	account, err = driver.ReadAccountByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", nil)
	c.EqualError(err, "dummy error")
	c.Empty(account)
}

func TestPostgresDriver_ReadAccounts(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "address", "height", "balance", "balance_denomination"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, "212121", "upokt").
		AddRow(2, "00353abd21ef72725b295ba5a9a5eb6082548e22", 21, "212121", "upokt")

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	accounts, err := driver.ReadAccounts(&ReadAccountsOptions{Page: 21, PerPage: 7, Height: 21})
	c.NoError(err)
	c.Len(accounts, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	accounts, err = driver.ReadAccounts(&ReadAccountsOptions{})
	c.EqualError(err, "dummy error")
	c.Empty(accounts)
}

func TestPostgresDriver_GetAccountsQuantity(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM accounts").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetAccountsQuantity(&GetAccountsQuantityOptions{Height: 21})
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM accounts").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetAccountsQuantity(nil)
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}

func TestPostgresDriver_WriteNodes(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	mock.ExpectExec("INSERT into nodes").WithArgs(pq.StringArray([]string{"00353abd21ef72725b295ba5a9a5eb6082548e21"}), pq.Int64Array([]int64{21}),
		pq.BoolArray([]bool{false}), pq.StringArray([]string{"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903"}),
		pq.StringArray([]string{"https://dummy.com:6045"}), pq.StringArray([]string{"212121"})).
		WillReturnResult(sqlmock.NewResult(1, 1))

	driver := NewPostgresDriverFromSQLDBInstance(db)

	nodesToSend := []*indexer.Node{
		{
			Address:    "00353abd21ef72725b295ba5a9a5eb6082548e21",
			Height:     21,
			Jailed:     false,
			PublicKey:  "01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903",
			ServiceURL: "https://dummy.com:6045",
			Tokens:     big.NewInt(212121),
		},
	}

	err = driver.WriteNodes(nodesToSend)
	c.NoError(err)

	mock.ExpectExec("INSERT into nodes").WithArgs(pq.StringArray([]string{"00353abd21ef72725b295ba5a9a5eb6082548e21"}), pq.Int64Array([]int64{21}),
		pq.BoolArray([]bool{false}), pq.StringArray([]string{"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903"}),
		pq.StringArray([]string{"https://dummy.com:6045"}), pq.StringArray([]string{"212121"})).
		WillReturnError(errors.New("dummy error"))

	err = driver.WriteNodes(nodesToSend)
	c.EqualError(err, "dummy error")
}

func TestPostgresDriver_ReadNodeByAddress(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "address", "height", "jailed", "public_key", "service_url", "tokens"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "https://dummy.com:6045", "212121")

	mock.ExpectQuery("^SELECT (.+) FROM nodes (.+)").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	node, err := driver.ReadNodeByAddress("00353abd21ef72725b295ba5a9a5eb6082548e2", &ReadNodeByAddressOptions{Height: 21})
	c.Equal(ErrInvalidAddress, err)
	c.Empty(node)

	node, err = driver.ReadNodeByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &ReadNodeByAddressOptions{Height: 21})
	c.NoError(err)
	c.NotEmpty(node)

	rows = sqlmock.NewRows([]string{"id", "address", "height", "jailed", "public_key", "service_url", "tokens"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "https://dummy.com:6045", "212121")

	mock.ExpectQuery("^SELECT (.+) FROM nodes (.+)").WillReturnRows(rows)

	node, err = driver.ReadNodeByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", nil)
	c.NoError(err)
	c.NotEmpty(node)

	mock.ExpectQuery("^SELECT (.+) FROM nodes (.+)").WillReturnError(errors.New("dummy error"))

	node, err = driver.ReadNodeByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &ReadNodeByAddressOptions{Height: 21})
	c.EqualError(err, "dummy error")
	c.Empty(node)

	mock.ExpectQuery("^SELECT (.+) FROM nodes (.+)").WillReturnError(errors.New("dummy error"))

	node, err = driver.ReadNodeByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", nil)
	c.EqualError(err, "dummy error")
	c.Empty(node)
}

func TestPostgresDriver_ReadNodes(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "address", "height", "jailed", "public_key", "service_url", "tokens"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "https://dummy.com:6045", "212121").
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e22", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "https://dummy.com:6043", "212121")

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	nodes, err := driver.ReadNodes(&ReadNodesOptions{Page: 21, PerPage: 7, Height: 21})
	c.NoError(err)
	c.Len(nodes, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	nodes, err = driver.ReadNodes(&ReadNodesOptions{})
	c.EqualError(err, "dummy error")
	c.Empty(nodes)
}

func TestPostgresDriver_GetNodesQuantity(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM nodes").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetNodesQuantity(&GetNodesQuantityOptions{Height: 21})
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM nodes").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetNodesQuantity(nil)
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}

func TestPostgresDriver_WriteApps(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	mock.ExpectExec("INSERT into apps").WithArgs(pq.StringArray([]string{"00353abd21ef72725b295ba5a9a5eb6082548e21"}), pq.Int64Array([]int64{21}),
		pq.BoolArray([]bool{false}), pq.StringArray([]string{"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903"}),
		pq.StringArray([]string{"212121"})).
		WillReturnResult(sqlmock.NewResult(1, 1))

	driver := NewPostgresDriverFromSQLDBInstance(db)

	appsToSend := []*indexer.App{
		{
			Address:      "00353abd21ef72725b295ba5a9a5eb6082548e21",
			Height:       21,
			Jailed:       false,
			PublicKey:    "01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903",
			StakedTokens: big.NewInt(212121),
		},
	}

	err = driver.WriteApps(appsToSend)
	c.NoError(err)

	mock.ExpectExec("INSERT into apps").WithArgs(pq.StringArray([]string{"00353abd21ef72725b295ba5a9a5eb6082548e21"}), pq.Int64Array([]int64{21}),
		pq.BoolArray([]bool{false}), pq.StringArray([]string{"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903"}),
		pq.StringArray([]string{"212121"})).
		WillReturnError(errors.New("dummy error"))

	err = driver.WriteApps(appsToSend)
	c.EqualError(err, "dummy error")
}

func TestPostgresDriver_ReadAppByAddress(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "address", "height", "jailed", "public_key", "staked_tokens"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "212121")

	mock.ExpectQuery("^SELECT (.+) FROM apps (.+)").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	app, err := driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e2", &ReadAppByAddressOptions{Height: 21})
	c.Equal(ErrInvalidAddress, err)
	c.Empty(app)

	app, err = driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &ReadAppByAddressOptions{Height: 21})
	c.NoError(err)
	c.NotEmpty(app)

	rows = sqlmock.NewRows([]string{"id", "address", "height", "jailed", "public_key", "staked_tokens"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "212121")

	mock.ExpectQuery("^SELECT (.+) FROM apps (.+)").WillReturnRows(rows)

	app, err = driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", nil)
	c.NoError(err)
	c.NotEmpty(app)

	mock.ExpectQuery("^SELECT (.+) FROM apps (.+)").WillReturnError(errors.New("dummy error"))

	app, err = driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &ReadAppByAddressOptions{Height: 21})
	c.EqualError(err, "dummy error")
	c.Empty(app)

	mock.ExpectQuery("^SELECT (.+) FROM apps (.+)").WillReturnError(errors.New("dummy error"))

	app, err = driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", nil)
	c.EqualError(err, "dummy error")
	c.Empty(app)
}

func TestPostgresDriver_ReadApps(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"id", "address", "height", "jailed", "public_key", "staked_tokens"}).
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e21", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "212121").
		AddRow(1, "00353abd21ef72725b295ba5a9a5eb6082548e22", 21, false,
			"01473af96ffc54c447f79d2fa06ee79e68c0dbd5b8257da25bf99dd89309c903", "212121")

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	apps, err := driver.ReadApps(&ReadAppsOptions{Page: 21, PerPage: 7, Height: 21})
	c.NoError(err)
	c.Len(apps, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	apps, err = driver.ReadApps(&ReadAppsOptions{})
	c.EqualError(err, "dummy error")
	c.Empty(apps)
}

func TestPostgresDriver_GetAppsQuantity(t *testing.T) {
	c := require.New(t)

	db, mock, err := sqlmock.New()
	c.NoError(err)

	defer db.Close()

	rows := sqlmock.NewRows([]string{"count"}).AddRow(100)

	mock.ExpectQuery("^SELECT (.+) FROM apps").WillReturnRows(rows)

	driver := NewPostgresDriverFromSQLDBInstance(db)

	maxHeight, err := driver.GetAppsQuantity(&GetAppsQuantityOptions{Height: 21})
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM apps").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetAppsQuantity(nil)
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}
