package workers

import (
	"context"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	"github.com/arcology-network/common-lib/common"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	evmCommon "github.com/arcology-network/evm/common"
	gatewayTypes "github.com/arcology-network/gateway-svc/service/types"
)

type LocalReceiver struct {
	actor.WorkerThread
}

//return a Subscriber struct
func NewLocalReceiver(concurrency int, groupid string) *LocalReceiver {
	in := LocalReceiver{}
	in.Set(concurrency, groupid)
	return &in
}

func (lr *LocalReceiver) OnStart() {
}

func (lr *LocalReceiver) OnMessageArrived(msgs []*actor.Message) error {
	return nil
}

func (lr *LocalReceiver) ReceivedTransactions(ctx context.Context, args *types.SendTransactionArgs, reply *types.SendTransactionReply) error {
	txLen := len(args.Txs)
	checkingtxs := make([][]byte, txLen)
	common.ParallelWorker(txLen, lr.Concurrency, lr.txWorker, args.Txs, &checkingtxs)
	txsPack := gatewayTypes.TxsPack{
		Txs: checkingtxs,
	}
	lr.MsgBroker.Send(actor.MsgTxLocalsUnChecked, &txsPack)

	reply.Status = 0
	return nil
}

func (lr *LocalReceiver) SendRawTransaction(ctx context.Context, args *types.RawTransactionArgs, reply *types.RawTransactionReply) error {
	txLen := 1
	checkingtxs := make([][]byte, txLen)
	common.ParallelWorker(txLen, lr.Concurrency, lr.txWorker, [][]byte{args.Txs}, &checkingtxs)
	txsPack := gatewayTypes.TxsPack{
		Txs:        checkingtxs,
		TxHashChan: make(chan ethCommon.Hash, 1),
	}
	lr.MsgBroker.Send(actor.MsgTxLocalsUnChecked, &txsPack)

	hash := <-txsPack.TxHashChan

	reply.TxHash = evmCommon.BytesToHash(hash.Bytes())
	return nil
}

func (lr *LocalReceiver) txWorker(start, end, idx int, args ...interface{}) {
	txs := args[0].([]interface{})[0].([][]byte)
	streamerTxs := args[0].([]interface{})[1].(*[][]byte)

	for i := start; i < end; i++ {
		tx := txs[i]
		sendingTx := make([]byte, len(tx)+1)
		bz := 0
		bz += copy(sendingTx[bz:], []byte{types.TxType_Eth})
		bz += copy(sendingTx[bz:], tx)
		(*streamerTxs)[i] = sendingTx
	}
}
