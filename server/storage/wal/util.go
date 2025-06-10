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
// 判断wal目录下面有没有wal文件 通过判断目录下的文件是不是wal后缀
// @Param dir wal目录
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
// 根据snap的index定位wal文件的目的是找到哪些wal文件内容不在snap中
// 也就是恢复数据靠的是两部分 snap+wal
// 这个地方定位的粒度是wal文件 所以并不是精确定位记录 也不需要
// 不怕文件找多了 就怕找少了
// 也就是说这个地方找到的wal文件 文件中的部分内容可以已经被打在了snap 但是没有关系 用wal回放的时候发现记录已经存在就跳过就行
// @Param names wal文件名 wal的文件名是seq-index.wal 已经按照seq升序排好了 也就是轮询的时候从后往前找wal文件先看新的wal文件 也就是index是大的 方便快速定位到要找的index在哪个wal文件
// @Param index 要找的index
// @Return 要找的log entry的index落在哪个wal文件 返回的是wal文件在slice中的脚标 0-based 没找到返回-1
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
// 校验wal文件 wal文件的seq必须严格单调递增 seq号不能有空洞
// @Param names 按照seq升序的wal文件名
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
// @Return wal文件名 按照seq升序排好了
func readWALNames(lg *zap.Logger, dirpath string) ([]string, error) {
	// default.etcd/member/wal目录下的文件 拿到的wal文件名都已经按照seq升序排好了
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

// wal日志文件名解析出index wal日志的后缀的wal 文件名前半部分表示wal日志序号 后半部分表示index x-x.wal
// @Return seq 文件名的前半部分表示seq 第几个wal文件 0-based
// @Return index 文件名的后半部分表示当前这个wal文件里面内容从哪个index开始的 0-based
func parseWALName(str string) (seq, index uint64, err error) {
	// wal日志
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, errBadWALName
	}
	// wal日志的文件名
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

// 生成wal文件名 seq-index.wal 设计成这样的目的是通过文件名可以达成两个效果
// 对seq排序就是对所有记录的排序
// 通过index就可以知道wal文件中记录的index范围 [上一个wal文件名的index...下一个wal文件名的index-1]
// @Param seq 递增序号 0-based 表示wal文件的顺序
// @Param index index号 0-based 表示当前wal文件里面存放的第一个log entry的index是多少 也就是wal文件里面内容从哪个index开始的
func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
