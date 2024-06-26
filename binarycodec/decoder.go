package binarycodec

import (
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type BinaryDecoder struct {
	encoders []binaryTypeEncoder
	fallback EncodeFunc
}

type EncodeFunc func([]byte) (any, error)

type binaryTypeEncoder struct {
	Type byte
	f    EncodeFunc
}

func NewBinaryDecoder() *BinaryDecoder {
	return &BinaryDecoder{}
}

func (b *BinaryDecoder) Register(registry *bsoncodec.Registry) {
	registry.RegisterTypeMapEntry(bson.TypeBinary, tDecodedBinary)
	registry.RegisterTypeDecoder(tDecodedBinary, b)
}

func (b *BinaryDecoder) WithSubtypeEncoder(binarySubtype byte, f EncodeFunc) *BinaryDecoder {
	b.encoders = append(b.encoders, binaryTypeEncoder{
		Type: binarySubtype,
		f:    f,
	})
	return b
}

func (b *BinaryDecoder) WithFallbackEncoder(f EncodeFunc) *BinaryDecoder {
	b.fallback = f
	return b
}

type DecodedBinary any

func (b *BinaryDecoder) DecodeValue(ctx bsoncodec.DecodeContext, vr bsonrw.ValueReader, val reflect.Value) error {
	if !val.CanSet() || val.Type() != tDecodedBinary {
		return bsoncodec.ValueDecoderError{Name: "customBinaryDecodeValue", Types: []reflect.Type{tDecodedBinary}, Received: val}
	}

	binaryDecoder, err := ctx.LookupDecoder(tPrimitiveBinary)
	if err != nil {
		return err
	}

	var primBinary primitive.Binary
	if err = binaryDecoder.DecodeValue(ctx, vr, reflect.ValueOf(&primBinary).Elem()); err != nil {
		return err
	}

	decoded, err := b.lookupEncoder(primBinary.Subtype)(primBinary.Data)
	if err != nil {
		return err
	}

	val.Set(reflect.ValueOf(DecodedBinary(decoded)))
	return nil
}

func (b *BinaryDecoder) lookupEncoder(bsonType byte) EncodeFunc {
	for _, encoder := range b.encoders {
		if encoder.Type == bsonType {
			return encoder.f
		}
	}

	if b.fallback != nil {
		return b.fallback
	}

	return NoopEncoder
}

var tDecodedBinary = reflect.TypeOf((*DecodedBinary)(nil)).Elem()
var tPrimitiveBinary = reflect.TypeOf(primitive.Binary{})
