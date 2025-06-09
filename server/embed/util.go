// Copyright 2016 The etcd Authors
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

package embed

import (
	"path/filepath"

	"go.etcd.io/etcd/server/v3/storage/wal"
)

// 看看wal目录有没有 没有就创建默认的目录default.etcd/member/wal
func isMemberInitialized(cfg *Config) bool {
	walDir := cfg.WalDir
	if walDir == "" {
		// 启动的时候没有指定wal目录就用默认的路径default.etcd/member/wal
		walDir = filepath.Join(cfg.Dir, "member", "wal")
	}
	return wal.Exist(walDir)
}
