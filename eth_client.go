package main

import (
	"context"
	"math/big"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

type ethClient interface {
	SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error)
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)
}

func createRealEthClient(host string, port string) (ethClient, error) {
	client, err := ethclient.Dial(host + ":" + port)
	if err != nil {
		return nil, err
	}

	return client, nil
}
