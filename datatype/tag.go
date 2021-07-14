package datatype

import (
	"gitlab.yunshan.net/yunshan/droplet-libs/codec"
)

type Tag struct {
	PolicyData PolicyData
}

func (t *Tag) Encode(encoder *codec.SimpleEncoder) {
	t.PolicyData.Encode(encoder)
}

func (t *Tag) Decode(decoder *codec.SimpleDecoder) {
	t.PolicyData.Decode(decoder)
}
