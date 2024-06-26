package binarycodec

import (
	"encoding/base64"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
)

var DefaultBinaryDecoder = NewBinaryDecoder().
	WithSubtypeEncoder(bson.TypeBinaryUUID, UUIDToStringEncoder).
	WithFallbackEncoder(Base64Encoder)

func UUIDToStringEncoder(data []byte) (any, error) {
	id, err := uuid.FromBytes(data)
	if err != nil {
		return nil, err
	}
	return id.String(), nil
}

func Base64Encoder(data []byte) (any, error) {
	return base64.StdEncoding.EncodeToString(data), nil
}

func NoopEncoder(data []byte) (any, error) {
	return data, nil
}
