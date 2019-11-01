package main

import "github.com/ethereum/go-ethereum/core/types"

type snsClient interface {
	broadcast(block *types.Block) error
}
