package workers

import (
	"context"

	ethCommon "github.com/arcology-network/3rd-party/eth/common"
	"github.com/arcology-network/common-lib/types"
	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/rpc"
	gatewayTypes "github.com/arcology-network/gateway-svc/service/types"
	"github.com/smallnest/rpcx/client"
)

type RpcClient struct {
	actor.WorkerThread
	xclient client.XClient
}

//return a Subscriber struct
func NewRpcClient(concurrency int, groupid string, zkServers []string) *RpcClient {
	rc := RpcClient{
		xclient: rpc.InitZookeeperRpcClient("tpp", zkServers),
	}
	rc.Set(concurrency, groupid)
	return &rc
}

func (rc *RpcClient) OnStart() {
}

func (rc *RpcClient) OnMessageArrived(msgs []*actor.Message) error {
	for _, v := range msgs {
		switch v.Name {
		case actor.MsgTxLocalsRpc:
			pack := v.Data.(*gatewayTypes.TxsPack)
			response := types.RawTransactionReply{}
			if len(pack.Txs) > 0 {
				rc.xclient.Call(context.Background(), "ReceivedTransactionFromRpc", &types.RawTransactionArgs{Txs: pack.Txs[0]}, &response)
				pack.TxHashChan <- *response.TxHash.(*ethCommon.Hash)
			} else {
				pack.TxHashChan <- ethCommon.Hash{}
			}

		}
	}
	return nil
}
