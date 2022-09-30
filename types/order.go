package types

// Order enum allows user to select order of returned results - desc or asc
type Order string

const (
	// DescendantOrder represents greater to lower order
	DescendantOrder Order = "desc"
	// AscendantOrder represents lower to greater order
	AscendantOrder Order = "asc"
)
