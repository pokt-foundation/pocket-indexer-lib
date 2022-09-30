package postgresdriver

import (
	"errors"
	"math/big"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/lib/pq"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
	"github.com/stretchr/testify/require"
)

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

	appsToSend := []*types.App{
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

	app, err := driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e2", &types.ReadAppByAddressOptions{Height: 21})
	c.Equal(ErrInvalidAddress, err)
	c.Empty(app)

	app, err = driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &types.ReadAppByAddressOptions{Height: 21})
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

	app, err = driver.ReadAppByAddress("00353abd21ef72725b295ba5a9a5eb6082548e21", &types.ReadAppByAddressOptions{Height: 21})
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

	apps, err := driver.ReadApps(&types.ReadAppsOptions{Page: 21, PerPage: 7, Height: 21})
	c.NoError(err)
	c.Len(apps, 2)

	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("dummy error"))
	mock.ExpectCommit()

	apps, err = driver.ReadApps(&types.ReadAppsOptions{})
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

	maxHeight, err := driver.GetAppsQuantity(&types.GetAppsQuantityOptions{Height: 21})
	c.NoError(err)
	c.Equal(int64(100), maxHeight)

	mock.ExpectQuery("^SELECT (.+) FROM apps").WillReturnError(errors.New("dummy error"))

	maxHeight, err = driver.GetAppsQuantity(nil)
	c.EqualError(err, "dummy error")
	c.Empty(maxHeight)
}
