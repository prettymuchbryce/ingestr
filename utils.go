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

/*
func marshalBlock(block *receiptsBlock, inclTx bool, fullTx bool) (map[string]interface{}, error) {
	fields := RPCMarshalHeader(block.Header())
	fields["size"] = hexutil.Uint64(block.Size())

	formatTx = func(tx *types.Transaction) (interface{}, error) {
		return newRPCTransactionFromBlockHash(block, tx.Hash()), nil
	}
	txs := block.Transactions()
	transactions := make([]interface{}, len(txs))
	var err error
	for i, tx := range txs {
		if transactions[i], err = formatTx(tx); err != nil {
			return nil, err
		}
	}
	fields["transactions"] = transactions
	uncles := block.Uncles()

	return fields, nil
}
*/

type receiptsBlock struct {
	Header       *types.Header        `json:"header"`
	Receipts     []*types.Receipt     `json:"receipts,omitempty"`
	Hash         common.Hash          `json:"hash"`
	Transactions []*types.Transaction `json:"transactions"`
}
