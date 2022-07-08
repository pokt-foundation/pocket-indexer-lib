package postgresdriver

import (
	"math/big"

	"github.com/lib/pq"
	"github.com/pokt-foundation/pocket-go/utils"
	indexer "github.com/pokt-foundation/pocket-indexer-lib"
)

const (
	insertNodesScript = `
	INSERT into nodes (address, height, jailed, public_key, service_url, tokens)
	(
		select * from unnest($1::text[], $2::int[], $3::boolean[], $4::text[], $5::text[], $6::numeric[])
	)`
	selectNodesScript = `
	DECLARE nodes_cursor CURSOR FOR SELECT * FROM nodes WHERE height = (SELECT MAX(height) FROM nodes);
	MOVE absolute %d from nodes_cursor;
	FETCH %d FROM nodes_cursor;
	`
	selectNodesByHeightScript = `
	DECLARE nodes_cursor CURSOR FOR SELECT * FROM nodes WHERE height = '%d';
	MOVE absolute %d from nodes_cursor;
	FETCH %d FROM nodes_cursor;
	`
	selectNodeByAddressScript          = "SELECT * FROM nodes WHERE address = $1 AND height = (SELECT MAX(height) FROM nodes)"
	selectNodeByAddressAndHeightScript = "SELECT * FROM nodes WHERE address = $1 AND height = $2"
	selectCountFromNodes               = "SELECT COUNT(*) FROM nodes WHERE height = (SELECT MAX(height) FROM nodes)"
	selectCountFromNodesByHeight       = "SELECT COUNT(*) FROM nodes WHERE height = $1"
)

// dbNode is struct handler for the node with types needed for Postgres processing
type dbNode struct {
	ID         int    `db:"id"`
	Address    string `db:"address"`
	Height     int    `db:"height"`
	Jailed     bool   `db:"jailed"`
	PublicKey  string `db:"public_key"`
	ServiceURL string `db:"service_url"`
	Tokens     string `db:"tokens"`
}

func (n *dbNode) toIndexerNode() *indexer.Node {
	tokens := new(big.Int)
	tokens, _ = tokens.SetString(n.Tokens, 10)

	return &indexer.Node{
		Address:    n.Address,
		Height:     n.Height,
		Jailed:     n.Jailed,
		PublicKey:  n.PublicKey,
		ServiceURL: n.ServiceURL,
		Tokens:     tokens,
	}
}

func convertIndexerNodeToDBNode(indexerNode *indexer.Node) *dbNode {
	return &dbNode{
		Address:    indexerNode.Address,
		Height:     indexerNode.Height,
		Jailed:     indexerNode.Jailed,
		PublicKey:  indexerNode.PublicKey,
		ServiceURL: indexerNode.ServiceURL,
		Tokens:     indexerNode.Tokens.String(),
	}
}

// WriteNodes inserts given nodes to the database
func (d *PostgresDriver) WriteNodes(nodes []*indexer.Node) error {
	var addresses, publicKeys, serviceURLs, allTokens []string
	var heights []int64
	var jaileds []bool

	for _, node := range nodes {
		dbNode := convertIndexerNodeToDBNode(node)
		addresses = append(addresses, dbNode.Address)
		heights = append(heights, int64(dbNode.Height))
		jaileds = append(jaileds, dbNode.Jailed)
		publicKeys = append(publicKeys, dbNode.PublicKey)
		serviceURLs = append(serviceURLs, dbNode.ServiceURL)
		allTokens = append(allTokens, dbNode.Tokens)
	}

	_, err := d.Exec(insertNodesScript, pq.StringArray(addresses),
		pq.Int64Array(heights),
		pq.BoolArray(jaileds),
		pq.StringArray(publicKeys),
		pq.StringArray(serviceURLs),
		pq.StringArray(allTokens))
	if err != nil {
		return err
	}

	return nil
}

// ReadNodeByAddressOptions optional parameters for ReadNodeByAddress
type ReadNodeByAddressOptions struct {
	Height int
}

// ReadNodeByAddress returns a node in the database with given address
func (d *PostgresDriver) ReadNodeByAddress(address string, options *ReadNodeByAddressOptions) (*indexer.Node, error) {
	if !utils.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	var dbNode dbNode
	var height int

	if options != nil {
		height = options.Height
	}

	if height == 0 {
		err := d.Get(&dbNode, selectNodeByAddressScript, address)
		if err != nil {
			return nil, err
		}
	} else {
		err := d.Get(&dbNode, selectNodeByAddressAndHeightScript, address, height)
		if err != nil {
			return nil, err
		}
	}

	return dbNode.toIndexerNode(), nil
}

// ReadNodesOptions optional parameters for ReadNodes
type ReadNodesOptions struct {
	PerPage int
	Page    int
	Height  int
}

// ReadNodes returns nodes with given height
// Optional values defaults: page: 1, perPage: 1000, height: last height
func (d *PostgresDriver) ReadNodes(options *ReadNodesOptions) ([]*indexer.Node, error) {
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

	query := getHeightOptionalQuery(selectNodesByHeightScript, selectNodesScript,
		height, move, perPage)

	var nodes []*dbNode

	err = tx.Select(&nodes, query)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	var indexerNodes []*indexer.Node

	for _, dbNode := range nodes {
		indexerNodes = append(indexerNodes, dbNode.toIndexerNode())
	}

	return indexerNodes, nil
}

// GetNodesQuantityOptions optinal params for GetNodesQuantity
type GetNodesQuantityOptions struct {
	Height int
}

// GetNodesQuantity returns quantity of nodes with given height saved
// default height is last height
func (d *PostgresDriver) GetNodesQuantity(options *GetNodesQuantityOptions) (int64, error) {
	var height int

	if options != nil {
		height = options.Height
	}

	row := d.getRowWithOptionalHeight(selectCountFromNodesByHeight, selectCountFromNodes, height)

	var quantity int64

	err := row.Scan(&quantity)
	if err != nil {
		return 0, err
	}

	return quantity, nil
}
