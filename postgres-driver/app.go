package postgresdriver

import (
	"math/big"

	"github.com/lib/pq"
	"github.com/pokt-foundation/pocket-go/utils"
	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

const (
	insertAppsScript = `
	INSERT into apps (address, height, jailed, public_key, staked_tokens)
	(
		select * from unnest($1::text[], $2::int[], $3::boolean[], $4::text[], $5::numeric[])
	)`
	selectAppsScript = `
	DECLARE apps_cursor CURSOR FOR SELECT * FROM apps WHERE height = (SELECT MAX(height) FROM apps);
	MOVE absolute %d from apps_cursor;
	FETCH %d FROM apps_cursor;
	`
	selectAppsByHeightScript = `
	DECLARE apps_cursor CURSOR FOR SELECT * FROM apps WHERE height = '%d';
	MOVE absolute %d from apps_cursor;
	FETCH %d FROM apps_cursor;
	`
	selectAppByAddressScript          = "SELECT * FROM apps WHERE address = $1 AND height = (SELECT MAX(height) FROM apps)"
	selectAppByAddressAndHeightScript = "SELECT * FROM apps WHERE address = $1 AND height = $2"
	selectCountFromApps               = "SELECT COUNT(*) FROM apps WHERE height = (SELECT MAX(height) FROM apps)"
	selectCountFromAppsByHeight       = "SELECT COUNT(*) FROM apps WHERE height = $1"
)

// dbApp is struct handler for the app with types needed for Postgres processing
type dbApp struct {
	ID           int    `db:"id"`
	Address      string `db:"address"`
	Height       int    `db:"height"`
	Jailed       bool   `db:"jailed"`
	PublicKey    string `db:"public_key"`
	StakedTokens string `db:"staked_tokens"`
}

func (a *dbApp) toIndexerApp() *types.App {
	stakedTokens := new(big.Int)
	stakedTokens, _ = stakedTokens.SetString(a.StakedTokens, 10)

	return &types.App{
		Address:      a.Address,
		Height:       a.Height,
		Jailed:       a.Jailed,
		PublicKey:    a.PublicKey,
		StakedTokens: stakedTokens,
	}
}

func convertIndexerAppToDBApp(indexerApp *types.App) *dbApp {
	return &dbApp{
		Address:      indexerApp.Address,
		Height:       indexerApp.Height,
		Jailed:       indexerApp.Jailed,
		PublicKey:    indexerApp.PublicKey,
		StakedTokens: indexerApp.StakedTokens.String(),
	}
}

// WriteApps inserts given apps to the database
func (d *PostgresDriver) WriteApps(apps []*types.App) error {
	var addresses, publicKeys, allStakedTokens []string
	var heights []int64
	var jaileds []bool

	for _, app := range apps {
		dbApp := convertIndexerAppToDBApp(app)
		addresses = append(addresses, dbApp.Address)
		heights = append(heights, int64(dbApp.Height))
		jaileds = append(jaileds, dbApp.Jailed)
		publicKeys = append(publicKeys, dbApp.PublicKey)
		allStakedTokens = append(allStakedTokens, dbApp.StakedTokens)
	}

	_, err := d.Exec(insertAppsScript, pq.StringArray(addresses),
		pq.Int64Array(heights),
		pq.BoolArray(jaileds),
		pq.StringArray(publicKeys),
		pq.StringArray(allStakedTokens))
	if err != nil {
		return err
	}

	return nil
}

// ReadAppByAddress returns an app in the database with given address
func (d *PostgresDriver) ReadAppByAddress(address string, options *types.ReadAppByAddressOptions) (*types.App, error) {
	if !utils.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	var dbApp dbApp
	var height int

	if options != nil {
		height = options.Height
	}

	if height == 0 {
		err := d.Get(&dbApp, selectAppByAddressScript, address)
		if err != nil {
			return nil, err
		}
	} else {
		err := d.Get(&dbApp, selectAppByAddressAndHeightScript, address, height)
		if err != nil {
			return nil, err
		}
	}

	return dbApp.toIndexerApp(), nil
}

// ReadApps returns apps with given height
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadApps(options *types.ReadAppsOptions) ([]*types.App, error) {
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

	query := getHeightOptionalQuery(selectAppsByHeightScript, selectAppsScript,
		height, move, perPage)

	var apps []*dbApp

	err = tx.Select(&apps, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerApps []*types.App

	for _, dbApp := range apps {
		indexerApps = append(indexerApps, dbApp.toIndexerApp())
	}

	return indexerApps, nil
}

// GetAppsQuantity returns quantity of apps with given height saved
// default height is last height
func (d *PostgresDriver) GetAppsQuantity(options *types.GetAppsQuantityOptions) (int64, error) {
	var height int

	if options != nil {
		height = options.Height
	}

	row := d.getRowWithOptionalHeight(selectCountFromAppsByHeight, selectCountFromApps, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}
