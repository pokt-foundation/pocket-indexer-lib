package types

type Transaction struct {
	Hash     string
	Sender   string
	Receiver string
	Amount   string
	Fee      string
	Height   int
	Code     int
	Status   string
	Memo     string
}
