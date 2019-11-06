package main

import (
	"encoding/json"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var millisecondNano = int64(1000000)

func msToDuration(ms int) time.Duration {
	return time.Duration(int64(ms) * millisecondNano)
}

func marshalReceiptBlock(block *receiptsBlock) (string, error) {
	resultBytes, err := json.Marshal(block)
	if err != nil {
		return "", err
	}

	return string(resultBytes), nil
}

func unmarshalReceiptBlock(blockJSON string) (*receiptsBlock, error) {
	var receiptsBlock *receiptsBlock

	err := json.Unmarshal([]byte(blockJSON), &receiptsBlock)

	if err != nil {
		return nil, err
	}

	return receiptsBlock, nil
}

type receiptsBlock struct {
	Header       *types.Header        `json:"header"`
	Receipts     []*types.Receipt     `json:"receipts"`
	Hash         common.Hash          `json:"hash"`
	Transactions []*types.Transaction `json:"transactions"`
}
