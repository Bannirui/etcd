// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
)

func main() {
	// 集群共识算法通信端口
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	// 节点标识
	id := flag.Int("id", 1, "node ID")
	// 客户端端口
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	// golang的chan我觉得本质是队列 实际用途既可以当作普通队列的内存缓冲区 又可以退化成常量级别的内存占用的信号量
	// 下面两个是典型的队列用途 用于解耦模块组件之间
	// 客户端put到httpKVAPI->kv store构建kv键值对->kv store把键值对发送到propose channel->raft node订阅propose channel负责同步和半数确认->发送到commit channel->kv store订阅commit channel后持久化
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	var kvs *kvstore
	// 内存中map序列化json
	getSnapshot := func() ([]byte, error) { return kvs.getSnapshot() }
	// 组件式思想 各司其职 raftNode负责raft算法实现 kvstore负责键值对数据库 httpKVAPI负责对客户端
	commitC, errorC, snapshotterReady := newRaftNode(*id, strings.Split(*cluster, ","), *join, getSnapshot, proposeC, confChangeC)

	kvs = newKVStore(<-snapshotterReady, proposeC, commitC, errorC)

	// the key-value http handler will propose updates to raft
	serveHTTPKVAPI(kvs, *kvport, confChangeC, errorC)
}
