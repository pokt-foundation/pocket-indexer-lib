package postgresdriver

import (
	"errors"
	"math/big"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
	"github.com/stretchr/testify/require"
)

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
