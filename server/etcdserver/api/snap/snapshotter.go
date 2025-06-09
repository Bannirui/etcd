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

package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/client/pkg/v3/verify"
	pioutil "go.etcd.io/etcd/pkg/v3/ioutil"
	"go.etcd.io/etcd/pkg/v3/pbutil"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap/snappb"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const snapSuffix = ".snap"

var (
	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")
	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	lg *zap.Logger
	// 快照目录default.etcd/member/snap
	dir string
}

func New(lg *zap.Logger, dir string) *Snapshotter {
	if lg == nil {
		lg = zap.NewNop()
	}
	return &Snapshotter{
		lg: lg,
		// default.etcd/member/snap
		dir: dir,
	}
}

func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}
	return s.save(&snapshot)
}

func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	start := time.Now()

	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)
	b := pbutil.MustMarshal(snapshot)
	crc := crc32.Update(0, crcTable, b)
	snap := snappb.Snapshot{Crc: crc, Data: b}
	d, err := snap.Marshal()
	if err != nil {
		return err
	}
	snapMarshallingSec.Observe(time.Since(start).Seconds())

	spath := filepath.Join(s.dir, fname)

	fsyncStart := time.Now()
	err = pioutil.WriteAndSyncFile(spath, d, 0o666)
	snapFsyncSec.Observe(time.Since(fsyncStart).Seconds())

	if err != nil {
		s.lg.Warn("failed to write a snap file", zap.String("path", spath), zap.Error(err))
		rerr := os.Remove(spath)
		if rerr != nil {
			s.lg.Warn("failed to remove a broken snap file", zap.String("path", spath), zap.Error(rerr))
		}
		return err
	}

	snapSaveSec.Observe(time.Since(start).Seconds())
	return nil
}

// Load returns the newest snapshot.
func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
	return s.loadMatching(func(*raftpb.Snapshot) bool { return true })
}

// LoadNewestAvailable loads the newest snapshot available that is in walSnaps.
// 为什么设计的这么复杂 不直接使用最新的snap文件作为恢复数据的依据呢 而是要跟wal进行比较
// 根本原因是要让wal认可snap 也就是保证恢复的数据一定是在wal中的
// 防止孤儿快照 也就是数据在snap中却不在wal中
// @Param walSnaps default.etcd/member/wal/目录下的wal文件
// @Return snap文件 raft服务器启动的时候用哪个snap文件作为数据恢复的依据
func (s *Snapshotter) LoadNewestAvailable(walSnaps []walpb.Snapshot) (*raftpb.Snapshot, error) {
	return s.loadMatching(func(snapshot *raftpb.Snapshot) bool {
		m := snapshot.Metadata
		for i := len(walSnaps) - 1; i >= 0; i-- {
			if m.Term == walSnaps[i].Term && m.Index == walSnaps[i].Index {
				return true
			}
		}
		return false
	})
}

// loadMatching returns the newest snapshot where matchFn returns true.
// @Param matchFn 用来筛选wal
func (s *Snapshotter) loadMatching(matchFn func(*raftpb.Snapshot) bool) (*raftpb.Snapshot, error) {
	// default.etcd/member/snap目录下所有的snap文件名 这个地方的文件名是按照快照时间降序的 为了下面先轮询到新快照文件设计的
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}
	var snap *raftpb.Snapshot
	for _, name := range names {
		// 从最新的快照snap中找到跟wal文件日志term和index的吻合的那个snap 也就是上一次快照的分水岭
		if snap, err = s.loadSnap(name); err == nil && matchFn(snap) {
			return snap, nil
		}
	}
	return nil, ErrNoSnapshot
}

// snap目录default.etcd/member/snap拼接文件名成snap文件的路径 读出来反序列化
// @Param name snap文件名
// @Return 反序列化出来的snap文件
func (s *Snapshotter) loadSnap(name string) (*raftpb.Snapshot, error) {
	// 目录+文件名拼接snap文件路径
	fpath := filepath.Join(s.dir, name)
	// 从snap文件中反序列化
	snap, err := Read(s.lg, fpath)
	if err != nil {
		brokenPath := fpath + ".broken"
		s.lg.Warn("failed to read a snap file", zap.String("path", fpath), zap.Error(err))
		if rerr := os.Rename(fpath, brokenPath); rerr != nil {
			s.lg.Warn("failed to rename a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath), zap.Error(rerr))
		} else {
			s.lg.Warn("renamed to a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath))
		}
	}
	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
// @Paran snapname snap文件路径
// @Return 反序列化出snap文件内容
func Read(lg *zap.Logger, snapname string) (*raftpb.Snapshot, error) {
	verify.Assert(lg != nil, "the logger should not be nil")
	// 读文件
	b, err := os.ReadFile(snapname)
	if err != nil {
		lg.Warn("failed to read a snap file", zap.String("path", snapname), zap.Error(err))
		return nil, err
	}

	if len(b) == 0 {
		lg.Warn("failed to read empty snapshot file", zap.String("path", snapname))
		return nil, ErrEmptySnapshot
	}

	var serializedSnap snappb.Snapshot
	// snap文件反序列化
	if err = serializedSnap.Unmarshal(b); err != nil {
		lg.Warn("failed to unmarshal snappb.Snapshot", zap.String("path", snapname), zap.Error(err))
		return nil, err
	}

	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		lg.Warn("failed to read empty snapshot data", zap.String("path", snapname))
		return nil, ErrEmptySnapshot
	}

	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		lg.Warn("snap file is corrupt",
			zap.String("path", snapname),
			zap.Uint32("prev-crc", serializedSnap.Crc),
			zap.Uint32("new-crc", crc),
		)
		return nil, ErrCRCMismatch
	}
	// 反序列化出来snap文件内容
	var snap raftpb.Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		lg.Warn("failed to unmarshal raftpb.Snapshot", zap.String("path", snapname), zap.Error(err))
		return nil, err
	}
	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
// default.etcd/member/snap目录下所的有snap文件名
// @Return snap文件名中有时间戳 返回出去的文件名是按照时间戳降序的 也就是先新快照
func (s *Snapshotter) snapNames() ([]string, error) {
	// snap目录default.etcd/member/snap
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	// 把snap目录下所有文件名读出来
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	// tmp文件不要
	filenames, err := s.cleanupSnapdir(names)
	if err != nil {
		return nil, err
	}
	// 只要.snap后缀的文件
	snaps := s.checkSuffix(filenames)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func (s *Snapshotter) checkSuffix(names []string) []string {
	var snaps []string
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a valid file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
				s.lg.Warn("found unexpected non-snap file; skipping", zap.String("path", names[i]))
			}
		}
	}
	return snaps
}

// cleanupSnapdir removes any files that should not be in the snapshot directory:
// - db.tmp prefixed files that can be orphaned by defragmentation
func (s *Snapshotter) cleanupSnapdir(filenames []string) (names []string, err error) {
	names = make([]string, 0, len(filenames))
	for _, filename := range filenames {
		if strings.HasPrefix(filename, "db.tmp") {
			s.lg.Info("found orphaned defragmentation file; deleting", zap.String("path", filename))
			if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
				return names, fmt.Errorf("failed to remove orphaned .snap.db file %s: %w", filename, rmErr)
			}
		} else {
			names = append(names, filename)
		}
	}
	return names, nil
}

func (s *Snapshotter) ReleaseSnapDBs(snap raftpb.Snapshot) error {
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, filename := range filenames {
		if strings.HasSuffix(filename, ".snap.db") {
			hexIndex := strings.TrimSuffix(filepath.Base(filename), ".snap.db")
			index, err := strconv.ParseUint(hexIndex, 16, 64)
			if err != nil {
				s.lg.Error("failed to parse index from filename", zap.String("path", filename), zap.String("error", err.Error()))
				continue
			}
			if index < snap.Metadata.Index {
				s.lg.Info("found orphaned .snap.db file; deleting", zap.String("path", filename))
				if rmErr := os.Remove(filepath.Join(s.dir, filename)); rmErr != nil && !os.IsNotExist(rmErr) {
					s.lg.Error("failed to remove orphaned .snap.db file", zap.String("path", filename), zap.String("error", rmErr.Error()))
				}
			}
		}
	}
	return nil
}
