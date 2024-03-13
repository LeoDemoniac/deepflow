package common

const (
	NETWORK_1M = iota
	NETWORK_1S
)

type ExportItem interface {
	DataSource() uint8
	EncodeTo(protocol int, dest interface{}) error
	Release()
	AddReferenceCountN(c int)
}
