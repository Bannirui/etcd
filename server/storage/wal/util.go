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

package wal

import (
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
)

var errBadWALName = errors.New("bad wal name")

// Exist returns true if there are any files in a given directory.
func Exist(dir string) bool {
	names, err := fileutil.ReadDir(dir, fileutil.WithExt(".wal"))
	if err != nil {
		return false
	}
	return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
func searchIndex(lg *zap.Logger, names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWALName(name)
		if err != nil {
			lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
func isValidSeq(lg *zap.Logger, names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWALName(name)
		if err != nil {
			lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}

// 找到wal目录下所有的wal日志文件
// @Param dirpath wal目录default.etcd/member/wal
// @Return wal文件名
func readWALNames(lg *zap.Logger, dirpath string) ([]string, error) {
	// default.etcd/member/wal目录下的文件
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, fmt.Errorf("[readWALNames] fileutil.ReadDir failed: %w", err)
	}
	// 后缀是wal的文件
	wnames := checkWALNames(lg, names)
	if len(wnames) == 0 {
		return nil, ErrFileNotFound
	}
	return wnames, nil
}

// 根据文件名的后缀筛选出wal文件
// @Return 后缀是wal的文件
func checkWALNames(lg *zap.Logger, names []string) []string {
	wnames := make([]string, 0)
	for _, name := range names {
		// 确保wal目录default.etcd/member/wal下的文件后缀都是wal 也就是保证都是wal日志文件
		if _, _, err := parseWALName(name); err != nil {
			// don't complain about left over tmp files
			if !strings.HasSuffix(name, ".tmp") {
				lg.Warn(
					"ignored file in WAL directory",
					zap.String("path", name),
				)
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

// wal日志文件名解析出index wal日志的后缀的wal 文件名前半部分表示term 后半部分表示index x-x.wal
// @Return seq 文件名的前半部分表示term
// @Return index 文件名的后半部分表示index
func parseWALName(str string) (seq, index uint64, err error) {
	// wal日志
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errBadWALName
	}
	// wal日志的文件名
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
