package workers

import (
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/log"
	"github.com/arcology-network/component-lib/storage"
	gatewayTypes "github.com/arcology-network/gateway-svc/service/types"
)

type TxRepeatedChecker struct {
	actor.WorkerThread
	checklist *storage.CheckedList
	waits     int64
	maxSize   int
}

//return a Subscriber struct
func NewTxRepeatedChecker(concurrency int, groupid string, maxSize, waitSeconds int) *TxRepeatedChecker {
	receiver := TxRepeatedChecker{}
	receiver.Set(concurrency, groupid)
	receiver.waits = int64(waitSeconds)
	receiver.maxSize = maxSize

	return &receiver
}

func (r *TxRepeatedChecker) OnStart() {
	r.checklist = storage.NewCheckList(r.waits)
}

func (r *TxRepeatedChecker) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgTxLocalsUnChecked:
			data := v.Data.(*gatewayTypes.TxsPack)
			r.checkRepeated(data, types.TxFrom_Local)
		case actor.MsgTxRemotes:
			pack := &gatewayTypes.TxsPack{
				Txs: v.Data.([][]byte),
			}
			r.checkRepeated(pack, types.TxFrom_Remote)
		case actor.MsgTxBlocks:
			pack := &gatewayTypes.TxsPack{
				Txs: v.Data.([][]byte),
			}
			r.checkRepeated(pack, types.TxFrom_Block)
		}
	}
	return nil
}

func (r *TxRepeatedChecker) checkRepeated(txspack *gatewayTypes.TxsPack, from byte) {
	txs := txspack.Txs
	txLen := len(txs)
	checkedTxs := make([][]byte, 0, txLen)
	logid := r.AddLog(log.LogLevel_Debug, "checkRepeated")
	interLog := r.GetLogger(logid)
	for i := range txs {
		if !r.checklist.ExistTx(txs[i], from, interLog) {
			tx := txs[i]
			sendingTx := make([]byte, len(tx)+1)
			bz := 0
			bz += copy(sendingTx[bz:], []byte{from})
			bz += copy(sendingTx[bz:], tx)

			checkedTxs = append(checkedTxs, sendingTx)
		}
	}
	//to other node with consensus
	if from == types.TxFrom_Local {
		r.MsgBroker.Send(actor.MsgTxLocals, txs)
	}

	//to tpp with rpc
	if txspack.TxHashChan != nil {
		r.MsgBroker.Send(actor.MsgTxLocalsRpc, &gatewayTypes.TxsPack{
			Txs:        checkedTxs,
			TxHashChan: txspack.TxHashChan,
		})
		return
	}
	//to tpp with kafka
	sendingTxs := make([][]byte, 0, r.maxSize)
	for i := range checkedTxs {
		if len(sendingTxs) >= r.maxSize {
			r.MsgBroker.Send(actor.MsgCheckedTxs, sendingTxs)
			sendingTxs = make([][]byte, 0, r.maxSize)
		} else {
			sendingTxs = append(sendingTxs, checkedTxs[i])
		}
	}
	if len(sendingTxs) > 0 {
		r.MsgBroker.Send(actor.MsgCheckedTxs, sendingTxs)
	}

}
