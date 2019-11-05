package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

var millisecondNano = int64(1000000)

func msToDuration(ms int) time.Duration {
	return time.Duration(int64(ms) * millisecondNano)
}

func marshalReceiptBlock(block *ReceiptsBlock) (string, error) {
	blockFields, err := marshalBlock(block.Block, true, true)
	if err != nil {
		return nil, err
	}

	blockFields["receipts"] = block.Receipts

	resultBytes, error := json.Marshal(blockFields)
	if err != nil {
		return nil, err
	}

	return string(resultBytes)
}

func unmarshalReceiptBlock(blockJSON string) (*ReceiptsBlock, error) {
	var block *types.Block
	var receipts []*types.Receipt

	block = unmarshalBlock

	return &ReceiptsBlock{
		Block:    block,
		Receipts: receipts,
	}
}

func marshalBlock(block *types.Block, inclTx bool, fullTx bool) (map[string]interface{}, error) {
	fields := RPCMarshalHeader(block.Header())
	fields["size"] = hexutil.Uint64(block.Size())

	if inclTx {
		formatTx := func(tx *types.Transaction) (interface{}, error) {
			return tx.Hash(), nil
		}
		if fullTx {
			formatTx = func(tx *types.Transaction) (interface{}, error) {
				return newRPCTransactionFromBlockHash(block, tx.Hash()), nil
			}
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
	}
	uncles := block.Uncles()
	uncleHashes := make([]common.Hash, len(uncles))
	for i, uncle := range uncles {
		uncleHashes[i] = uncle.Hash()
	}
	fields["uncles"] = uncleHashes

	return fields, nil
}

func unmarshalBlock(blockJSON string) (*types.Block, error) {
	var raw json.RawMessage
	err := json.Unmarshal([]byte(blockJSON), &raw)
	if err != nil {
		return nil, err
	}
	// Decode header and transactions.
	var head *types.Header
	var body rpcBlock
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	// Quick-verify transaction and uncle lists. This mostly helps with debugging the server.
	if head.TxHash == types.EmptyRootHash && len(body.Transactions) > 0 {
		return nil, fmt.Errorf("server returned non-empty transaction list but block header indicates no transactions")
	}
	if head.TxHash != types.EmptyRootHash && len(body.Transactions) == 0 {
		return nil, fmt.Errorf("server returned empty transaction list but block header indicates transactions")
	}
	// Fill the sender cache of transactions in the block.
	txs := make([]*types.Transaction, len(body.Transactions))
	for i, tx := range body.Transactions {
		txs[i] = tx
	}
	return types.NewBlockWithHeader(head).WithBody(txs, []*types.Header{}), nil
}

type rpcBlock struct {
	Hash         common.Hash          `json:"hash"`
	Transactions []*types.Transaction `json:"transactions"`
	UncleHashes  []common.Hash        `json:"uncles"`
}

type txExtraInfo struct {
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

type receiptsBlock struct {
	Header       *types.Header        `json:"header"`
	Receipts     []*types.Receipt     `json:"receipts,omitempty"`
	Hash         common.Hash          `json:"hash"`
	Transactions []*types.Transaction `json:"transactions"`
	UncleHashes  []common.Hash        `json:"uncles"`
}
