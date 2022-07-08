package postgresdriver

import (
	"math/big"

	"github.com/pokt-foundation/pocket-go/utils"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
)

const (
	insertAccountScript = `
	INSERT into accounts (address, height, account_type, balance, balance_denomination)
	VALUES (:address, :height, :account_type, :balance, :balance_denomination)`
	selectAccountsScript = `
	DECLARE accounts_cursor CURSOR FOR SELECT * FROM accounts WHERE height = (SELECT MAX(height) FROM accounts);
	MOVE absolute %d from accounts_cursor;
	FETCH %d FROM accounts_cursor;
	`
	selectAccountsByHeightScript = `
	DECLARE accounts_cursor CURSOR FOR SELECT * FROM accounts WHERE height = '%d';
	MOVE absolute %d from accounts_cursor;
	FETCH %d FROM accounts_cursor;
	`
	selectAccountByAddressScript          = "SELECT * FROM accounts WHERE address = $1 AND height = (SELECT MAX(height) FROM accounts)"
	selectAccountByAddressAndHeightScript = "SELECT * FROM accounts WHERE address = $1 AND height = $2"
	selectCountFromAccounts               = "SELECT COUNT(*) FROM accounts WHERE height = (SELECT MAX(height) FROM accounts)"
	selectCountFromAccountsByHeight       = "SELECT COUNT(*) FROM accounts WHERE height = $1"
)

// dbAccount is struct handler for the account with types needed for Postgres processing
type dbAccount struct {
	ID                  int    `db:"id"`
	Address             string `db:"address"`
	Height              int    `db:"height"`
	AccountType         string `db:"account_type"`
	Balance             string `db:"balance"`
	BalanceDenomination string `db:"balance_denomination"`
}

func (a *dbAccount) toIndexerAccount() *indexer.Account {
	balance := new(big.Int)
	balance, _ = balance.SetString(a.Balance, 10)

	return &indexer.Account{
		Address:             a.Address,
		Height:              a.Height,
		AccountType:         indexer.AccountType(a.AccountType),
		Balance:             balance,
		BalanceDenomination: a.BalanceDenomination,
	}
}

func convertIndexerAccountToDBAccount(indexerAccount *indexer.Account) *dbAccount {
	return &dbAccount{
		Address:             indexerAccount.Address,
		Height:              indexerAccount.Height,
		AccountType:         string(indexerAccount.AccountType),
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

// ReadAccountByAddressOptions optional parameters for ReadAccountByAddress
type ReadAccountByAddressOptions struct {
	Height int
}

// ReadAccountByAddress returns an account in the database with given address
func (d *PostgresDriver) ReadAccountByAddress(address string, options *ReadAccountByAddressOptions) (*indexer.Account, error) {
	if !utils.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	var dbAccount dbAccount
	var height int

	if options != nil {
		height = options.Height
	}

	if height == 0 {
		err := d.Get(&dbAccount, selectAccountByAddressScript, address)
		if err != nil {
			return nil, err
		}
	} else {
		err := d.Get(&dbAccount, selectAccountByAddressAndHeightScript, address, height)
		if err != nil {
			return nil, err
		}
	}

	return dbAccount.toIndexerAccount(), nil
}

// ReadAccountsOptions optional parameters for ReadAccounts
type ReadAccountsOptions struct {
	PerPage int
	Page    int
	Height  int
}

// ReadAccounts returns accounts with given height
// Optional values defaults: page: 1, perPage: 1000, height: last height
func (d *PostgresDriver) ReadAccounts(options *ReadAccountsOptions) ([]*indexer.Account, error) {
	perPage := defaultPerPage
	page := defaultPage
	height := 0

	if options != nil {
		perPage = getPerPageValue(options.PerPage)
		page = getPageValue(options.Page)
		height = options.Height
	}

	move := getMoveValue(perPage, page)

	tx, err := d.Beginx()
	if err != nil {
		return nil, err
	}

	query := getHeightOptionalQuery(selectAccountsByHeightScript, selectAccountsScript,
		height, move, perPage)

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

// GetAccountsQuantityOptions optional parameters for GetAccountsQuantity
type GetAccountsQuantityOptions struct {
	Height int
}

// GetAccountsQuantity returns quantity of accounts with given height saved
// default height is last height
func (d *PostgresDriver) GetAccountsQuantity(options *GetAccountsQuantityOptions) (int64, error) {
	var height int

	if options != nil {
		height = options.Height
	}

	row := d.getRowWithOptionalHeight(selectCountFromAccountsByHeight, selectCountFromAccounts, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}
