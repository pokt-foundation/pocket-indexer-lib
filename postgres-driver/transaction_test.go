package postgresdriver

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"math/big"
	"testing"

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
