package workers

import (
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/log"
	"github.com/arcology-network/component-lib/storage"
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
		case actor.MsgTxLocals:
			data := v.Data.([][]byte)
			r.checkRepeated(data, types.TxFrom_Local)
		case actor.MsgTxRemotes:
			data := v.Data.([][]byte)
			r.checkRepeated(data, types.TxFrom_Remote)
		case actor.MsgTxBlocks:
			data := v.Data.([][]byte)
			r.checkRepeated(data, types.TxFrom_Block)
		}
	}
	return nil
}

func (r *TxRepeatedChecker) checkRepeated(txs [][]byte, from byte) {
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
