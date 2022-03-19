package service

import (
	"net/http"

	"github.com/arcology-network/component-lib/actor"
	"github.com/arcology-network/component-lib/rpc"
	"github.com/arcology-network/component-lib/streamer"
	"github.com/arcology-network/gateway-svc/service/workers"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"github.com/arcology-network/component-lib/kafka"
)

type Config struct {
	concurrency int
	groupid     string
}

//return a Subscriber struct
func NewConfig() *Config {
	return &Config{
		concurrency: viper.GetInt("concurrency"),
		groupid:     "gateway",
	}
}

func (cfg *Config) Start() {

	http.Handle("/streamer", promhttp.Handler())
	go http.ListenAndServe(":19002", nil)

	broker := streamer.NewStatefulStreamer()
	//00 initializer
	initializer := actor.NewActor(
		"initializer",
		broker,
		[]string{actor.MsgStarting},
		[]string{
			actor.MsgStartSub,
		},
		[]int{1},
		workers.NewInitializer(cfg.concurrency, cfg.groupid),
	)
	initializer.Connect(streamer.NewDisjunctions(initializer, 1))

	//01 kafkaDownloader
	receiveMseeages := []string{
		//actor.MsgTxLocals,
		actor.MsgTxRemotes,
	}
	receiveTopics := []string{
		viper.GetString("remote-txs"),
		//viper.GetString("local-txs"),
	}
	kafkaDownloader := actor.NewActor(
		"kafkaDownloader",
		broker,
		[]string{actor.MsgStartSub},
		receiveMseeages,
		[]int{1000, 1000},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, receiveTopics, receiveMseeages, viper.GetString("mqaddr2")),
	)
	kafkaDownloader.Connect(streamer.NewDisjunctions(kafkaDownloader, 1000))

	//02 kafkaDownloader
	kafkaDownloaderBlock := actor.NewActor(
		"kafkaDownloaderBlock",
		broker,
		[]string{actor.MsgStartSub},
		[]string{actor.MsgTxBlocks},
		[]int{100},
		kafka.NewKafkaDownloader(cfg.concurrency, cfg.groupid, []string{viper.GetString("block-txs")}, []string{actor.MsgTxBlocks}, viper.GetString("mqaddr")),
	)
	kafkaDownloaderBlock.Connect(streamer.NewDisjunctions(kafkaDownloaderBlock, 1000))

	//03 localReceiver
	receiveWorker := workers.NewLocalReceiver(cfg.concurrency, cfg.groupid)
	localReceiver := actor.NewActor(
		"localReceiver",
		broker,
		[]string{actor.MsgStartSub},
		[]string{
			actor.MsgTxLocalsUnChecked,
		},
		[]int{100},
		receiveWorker,
	)
	localReceiver.Connect(streamer.NewDisjunctions(localReceiver, 1))

	//04 txRepeatedChecker
	txRepeatedChecker := actor.NewActor(
		"txRepeatedChecker",
		broker,
		[]string{
			actor.MsgTxLocalsUnChecked,
			actor.MsgTxBlocks,
			actor.MsgTxRemotes,
		},
		[]string{
			actor.MsgCheckedTxs,
			actor.MsgTxLocals,
			actor.MsgTxLocalsRpc,
		},
		[]int{100, 100, 100},
		workers.NewTxRepeatedChecker(cfg.concurrency, cfg.groupid, viper.GetInt("txnums"), viper.GetInt("waits")),
	)
	txRepeatedChecker.Connect(streamer.NewDisjunctions(txRepeatedChecker, 100))

	//05 rpc_client
	rpcClient := actor.NewActor(
		"rpcClient",
		broker,
		[]string{
			actor.MsgTxLocalsRpc,
		},
		[]string{},
		[]int{100},
		workers.NewRpcClient(cfg.concurrency, cfg.groupid, []string{viper.GetString("zkUrl")}),
	)
	rpcClient.Connect(streamer.NewDisjunctions(rpcClient, 100))

	//04 kafkaUploader
	relations := map[string]string{}
	relations[actor.MsgCheckedTxs] = viper.GetString("checked-txs")
	relations[actor.MsgTxLocals] = viper.GetString("local-txs")
	kafkaUploader := actor.NewActor(
		"kafkaUploader",
		broker,
		[]string{
			actor.MsgCheckedTxs,
			actor.MsgTxLocals,
		},
		[]string{},
		[]int{100},
		kafka.NewKafkaUploader(cfg.concurrency, cfg.groupid, relations, viper.GetString("mqaddr2")),
	)
	kafkaUploader.Connect(streamer.NewDisjunctions(kafkaUploader, 1000))

	//starter
	selfStarter := streamer.NewDefaultProducer("selfStarter", []string{actor.MsgStarting}, []int{1})
	broker.RegisterProducer(selfStarter)
	broker.Serve()

	// rpcx service
	rpc.InitZookeeperRpcServer(viper.GetString("localIp")+":8975", "gateway", []string{viper.GetString("zkUrl")}, []interface{}{receiveWorker}, nil)

	//start signel
	streamerStarting := actor.Message{
		Name:   actor.MsgStarting,
		Height: 0,
		Round:  0,
		Data:   "start",
	}
	broker.Send(actor.MsgStarting, &streamerStarting)

}

func (cfg *Config) Stop() {

}
