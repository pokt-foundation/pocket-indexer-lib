// Package postgresdriver is the implementation of Writer interface for the Indexer using postgres SQL as persistance
package postgresdriver

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// Order enum allows user to select order of returned results - desc or asc
type Order string

const (
	// DescendantOrder represents greater to lower order
	DescendantOrder Order = "desc"
	// AscendantOrder represents lower to greater order
	AscendantOrder Order = "asc"
)

const (
	defaultPerPage = 1000
	defaultPage    = 1
	defaultOrder   = DescendantOrder
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

func newSQLNullString(value string) sql.NullString {
	if value == "" {
		return sql.NullString{}
	}

	return sql.NullString{
		String: value,
		Valid:  true,
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

func getOrderValue(optionsOrder Order) Order {
	if optionsOrder == "" {
		return defaultOrder
	}

	return optionsOrder
}

func getMoveValue(perPage, page int) int {
	return (page - 1) * perPage
}

func getHeightOptionalQuery(queryWithHeight, queryWithoutHeight string, height, move, perPage int) string {
	if height == 0 {
		return fmt.Sprintf(queryWithoutHeight, move, perPage)
	}

	return fmt.Sprintf(queryWithHeight, height, move, perPage)
}

func (d *PostgresDriver) getRowWithOptionalHeight(queryWithHeight, queryWithoutHeight string, height int) *sql.Row {
	if height == 0 {
		return d.QueryRow(queryWithoutHeight)
	}

	return d.QueryRow(queryWithHeight, height)
}
