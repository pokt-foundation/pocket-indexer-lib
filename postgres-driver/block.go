package postgresdriver

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/pokt-foundation/pocket-indexer-lib/types"
)

const (
	insertBlockScript = `
	INSERT into blocks (hash, height, time, proposer_address, tx_count, tx_total)
	VALUES (:hash, :height, :time, :proposer_address, :tx_count, :tx_total)`
	updateBlockCalculatedFieldsScript = `
	UPDATE blocks
	SET accounts_quantity = :accounts_quantity, apps_quantity = :apps_quantity, nodes_quantity = :nodes_quantity, took = :took
	WHERE height = :height`
	selectBlocksScript = `
	DECLARE blocks_cursor CURSOR FOR SELECT * FROM blocks ORDER BY height %s;
	MOVE absolute %d from blocks_cursor;
	FETCH %d FROM blocks_cursor;
	`
	selectBlockByHashScript      = "SELECT * FROM blocks WHERE hash = $1"
	selectBlockByHeightScript    = "SELECT * FROM blocks WHERE height = $1"
	selectBlockByMaxHeightScript = "SELECT * FROM blocks WHERE height = (SELECT MAX(height) FROM blocks)"
	selectCountFromBlocks        = "SELECT COUNT(*) FROM blocks"
	selectMaxHeightFromBlocks    = "SELECT MAX(height) FROM blocks"
)

// dbBlock is struct handler for the block with types needed for Postgres processing
type dbBlock struct {
	ID               int       `db:"id"`
	Hash             string    `db:"hash"`
	Height           int       `db:"height"`
	Time             time.Time `db:"time"`
	ProposerAddress  string    `db:"proposer_address"`
	TXCount          int       `db:"tx_count"`
	TXTotal          int       `db:"tx_total"`
	AccountsQuantity int       `db:"accounts_quantity"`
	AppsQuantity     int       `db:"apps_quantity"`
	NodesQuantity    int       `db:"nodes_quantity"`
	Took             string    `db:"took"`
}

func (b *dbBlock) toIndexerBlock() *types.Block {
	took, _ := strconv.Atoi(b.Took)

	return &types.Block{
		Hash:             b.Hash,
		Height:           b.Height,
		Time:             b.Time,
		ProposerAddress:  b.ProposerAddress,
		TXCount:          b.TXCount,
		TXTotal:          b.TXTotal,
		AccountsQuantity: b.AccountsQuantity,
		AppsQuantity:     b.AppsQuantity,
		NodesQuantity:    b.NodesQuantity,
		Took:             time.Duration(took),
	}
}

func convertIndexerBlockToDBBlock(indexerBlock *types.Block) *dbBlock {
	return &dbBlock{
		Hash:            indexerBlock.Hash,
		Height:          indexerBlock.Height,
		Time:            indexerBlock.Time,
		ProposerAddress: indexerBlock.ProposerAddress,
		TXCount:         indexerBlock.TXCount,
		TXTotal:         indexerBlock.TXTotal,
	}
}

// WriteBlock inserts given block to the database
func (d *PostgresDriver) WriteBlock(block *types.Block) error {
	dbBlock := convertIndexerBlockToDBBlock(block)

	_, err := d.NamedExec(insertBlockScript, dbBlock)
	if err != nil {
		return err
	}

	return nil
}

type updateBlockCalculatedFields struct {
	Height           int    `db:"height"`
	AccountsQuantity int    `db:"accounts_quantity"`
	AppsQuantity     int    `db:"apps_quantity"`
	NodesQuantity    int    `db:"nodes_quantity"`
	Took             string `db:"took"`
}

func extractCalculatedFields(block *types.Block) *updateBlockCalculatedFields {
	return &updateBlockCalculatedFields{
		Height:           block.Height,
		AccountsQuantity: block.AccountsQuantity,
		AppsQuantity:     block.AppsQuantity,
		NodesQuantity:    block.NodesQuantity,
		Took:             strconv.Itoa(int(block.Took)),
	}
}

// WriteBlockCalculatedFields writes block calculated fields (quantities and took)
func (d *PostgresDriver) WriteBlockCalculatedFields(block *types.Block) error {
	calculatedFields := extractCalculatedFields(block)

	_, err := d.NamedExec(updateBlockCalculatedFieldsScript, calculatedFields)
	if err != nil {
		return err
	}

	return nil
}

// ReadBlocks returns all blocks on the database with pagination
// Optional values defaults: page: 1, perPage: 1000
func (d *PostgresDriver) ReadBlocks(options *types.ReadBlocksOptions) ([]*types.Block, error) {
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

	query := fmt.Sprintf(selectBlocksScript, order, move, perPage)

	var blocks []*dbBlock

	err = tx.Select(&blocks, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerBlocks []*types.Block

	for _, block := range blocks {
		indexerBlocks = append(indexerBlocks, block.toIndexerBlock())
	}

	return indexerBlocks, nil
}

// ReadBlockByHash returns block in the database with given block hash
func (d *PostgresDriver) ReadBlockByHash(hash string) (*types.Block, error) {
	var dbBlock dbBlock

	err := d.Get(&dbBlock, selectBlockByHashScript, hash)
	if err != nil {
		return nil, err
	}

	return dbBlock.toIndexerBlock(), nil
}

// ReadBlockByHeight returns block in the database with given height
// height 0 is last height
func (d *PostgresDriver) ReadBlockByHeight(height int) (*types.Block, error) {
	var dbBlock dbBlock

	if height == 0 {
		err := d.Get(&dbBlock, selectBlockByMaxHeightScript)
		if err != nil {
			return nil, err
		}
	} else {
		err := d.Get(&dbBlock, selectBlockByHeightScript, height)
		if err != nil {
			return nil, err
		}
	}

	return dbBlock.toIndexerBlock(), nil
}

// GetMaxHeightInBlocks returns max height saved on blocks' table
func (d *PostgresDriver) GetMaxHeightInBlocks() (int64, error) {
	row := d.QueryRow(selectMaxHeightFromBlocks)

	var maxHeight sql.NullInt64

	err := row.Scan(&maxHeight)
	if err != nil {
		return 0, err
	}

	if !maxHeight.Valid {
		return 0, ErrNoPreviousHeight
	}

	return maxHeight.Int64, nil
}

// GetBlocksQuantity returns quantity of blocks saved
func (d *PostgresDriver) GetBlocksQuantity() (int64, error) {
	row := d.QueryRow(selectCountFromBlocks)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}
