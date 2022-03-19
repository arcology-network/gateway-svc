package types

import (
	ethCommon "github.com/arcology-network/3rd-party/eth/common"
)

type TxsPack struct {
	Txs        [][]byte
	TxHashChan chan ethCommon.Hash
}
