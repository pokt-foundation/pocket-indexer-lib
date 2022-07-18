package postgresdriver

import (
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
	postgresdriver "github.com/pokt-foundation/pocket-indexer-lib/postgres-driver"
	"github.com/stretchr/testify/require"
)

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
		AddRow(1, "EDFG", 22, time.Date(1999, time.July, 21, 0, 0, 0, 0, time.Local), "ABCD", 21)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnRows(rows)
	mock.ExpectCommit()

	driver := NewPostgresDriverFromSQLDBInstance(db)

	blocks, err := driver.ReadBlocks(&ReadBlocksOptions{Page: 1, PerPage: 7, Order: postgresdriver.DescendantOrder})
	c.NoError(err)
	c.Len(blocks, 2)
	c.Equal(blocks[0].Hash, "ABCD")

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
