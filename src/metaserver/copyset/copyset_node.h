/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Date: Fri Aug  6 17:10:54 CST 2021
 * Author: wuhanqing
 */

#ifndef DINGOFS_SRC_METASERVER_COPYSET_COPYSET_NODE_H_
#define DINGOFS_SRC_METASERVER_COPYSET_COPYSET_NODE_H_

#include <braft/raft.h>

#include <atomic>
#include <cstdint>
#include <list>
#include <memory>
#include <string>

#include "metaserver/common/types.h"
#include "metaserver/copyset/apply_queue.h"
#include "metaserver/copyset/conf_epoch_file.h"
#include "metaserver/copyset/config.h"
#include "metaserver/copyset/copyset_conf_change.h"
#include "metaserver/copyset/metric.h"
#include "metaserver/copyset/raft_node.h"
#include "metaserver/metastore.h"

namespace dingofs {
namespace metaserver {
namespace copyset {

class CopysetNodeManager;

// Implement our own business raft state machine
class CopysetNode : public braft::StateMachine {
 public:
  CopysetNode(PoolId poolId, CopysetId copysetId,
              const braft::Configuration& conf,
              CopysetNodeManager* nodeManager);

  ~CopysetNode() override;

  bool Init(const CopysetNodeOptions& options);

  /**
   * @brief Start raft node
   */
  bool Start();

  void Stop();

  /**
   * @brief Propose an op request to copyset node
   */
  virtual void Propose(const braft::Task& task);

  int64_t LeaderTerm() const;

  virtual bool IsLeaderTerm() const;

  PoolId GetPoolId() const;

  const braft::PeerId& GetPeerId() const;

  CopysetId GetCopysetId() const;

  braft::PeerId GetLeaderId() const;

  MetaStore* GetMetaStore() const;

  virtual uint64_t GetConfEpoch() const;

  std::string GetCopysetDataDir() const;

  void UpdateAppliedIndex(uint64_t index);

  uint64_t GetAppliedIndex() const;

  /**
   * @brief Get current copyset node's leader status
   * @return true if success, otherwise return false
   */
  bool GetLeaderStatus(braft::NodeStatus* leaderStatus);

  /**
   * @brief Get current copyset node's status
   */
  void GetStatus(braft::NodeStatus* status);

  virtual void ListPeers(std::vector<pb::common::Peer>* peers) const;

  ApplyQueue* GetApplyQueue() const;

  OperatorMetric* GetMetric() const;

  const std::string& Name() const;

  int64_t LatestLoadSnapshotIndex() const;

  int LoadConfEpoch(const std::string& file);

  int SaveConfEpoch(const std::string& file);

  void TEST_SetMetaStore(MetaStore* metastore) { metaStore_.reset(metastore); }

  void TEST_FlushApplyQueue() { applyQueue_->Flush(); }

  void TEST_SetRaftNode(RaftNode* raftNode) { raftNode_.reset(raftNode); }

 public:
  /** configuration change interfaces **/

  virtual butil::Status TransferLeader(const pb::common::Peer& target);
  virtual void AddPeer(const pb::common::Peer& peer,
                       braft::Closure* done = nullptr);
  virtual void RemovePeer(const pb::common::Peer& peer,
                          braft::Closure* done = nullptr);
  virtual void ChangePeers(const std::vector<pb::common::Peer>& newPeers,
                           braft::Closure* done = nullptr);
  void GetConfChange(pb::mds::heartbeat::ConfigChangeType* type,
                     pb::common::Peer* alterPeer);
  void OnConfChangeComplete();

 private:
  // Whether current copyset is ready do configuration change
  // return butil::Status::OK() if ready
  butil::Status ReadyDoConfChange();
  bool HasOngoingConfChange();

  void DoAddOrRemovePeer(pb::mds::heartbeat::ConfigChangeType type,
                         const pb::common::Peer& peer, braft::Closure* done);

 public:
  /*** implement interfaces from braft::StateMacine ***/

  void on_apply(braft::Iterator& iter) override;

  void on_shutdown() override;

  void on_snapshot_save(braft::SnapshotWriter* writer,
                        braft::Closure* done) override;

  int on_snapshot_load(braft::SnapshotReader* reader) override;

  void on_leader_start(int64_t term) override;

  void on_leader_stop(const butil::Status& status) override;

  void on_error(const braft::Error& e) override;

  void on_configuration_committed(const braft::Configuration& conf,
                                  int64_t index) override;

  void on_stop_following(const braft::LeaderChangeContext& ctx) override;

  void on_start_following(const braft::LeaderChangeContext& ctx) override;

 public:
  // for heartbeat
  bool GetPartitionInfoList(
      std::list<pb::common::PartitionInfo>* partitionInfoList);

  bool IsLoading() const;

 private:
  void InitRaftNodeOptions();

  bool FetchLeaderStatus(const braft::PeerId& peerId,
                         braft::NodeStatus* leaderStatus);

 private:
  const PoolId poolId_;
  const CopysetId copysetId_;
  const GroupId groupId_;

  // copyset name: (poolid, copysetid, groupid)
  const std::string name_;

  // configuration of current copyset
  braft::Configuration conf_;

  CopysetNodeManager* nodeManager_;

  // configuration version of current copyset
  std::atomic<uint64_t> epoch_;

  CopysetNodeOptions options_;

  // current term, greater than 0 means leader
  std::atomic<int64_t> leaderTerm_;

  braft::PeerId peerId_;

  std::unique_ptr<RaftNode> raftNode_;

  std::string copysetDataPath_;

  std::unique_ptr<MetaStore> metaStore_;

  // applied log index
  std::atomic<uint64_t> appliedIndex_;

  std::unique_ptr<ConfEpochFile> epochFile_;

  std::unique_ptr<ApplyQueue> applyQueue_;

  mutable Mutex confMtx_;

  int64_t latestLoadSnapshotIndex_;

  mutable Mutex confChangeMtx_;

  OngoingConfChange ongoingConfChange_;

  std::unique_ptr<OperatorMetric> metric_;

  std::atomic<bool> isLoading_;
};

inline void CopysetNode::Propose(const braft::Task& task) {
  raftNode_->apply(task);
}

inline int64_t CopysetNode::LeaderTerm() const {
  return leaderTerm_.load(std::memory_order_acquire);
}

inline bool CopysetNode::IsLeaderTerm() const {
  return leaderTerm_.load(std::memory_order_acquire) > 0;
}

inline PoolId CopysetNode::GetPoolId() const { return poolId_; }

inline CopysetId CopysetNode::GetCopysetId() const { return copysetId_; }

inline braft::PeerId CopysetNode::GetLeaderId() const {
  return raftNode_->leader_id();
}

inline MetaStore* CopysetNode::GetMetaStore() const { return metaStore_.get(); }

inline uint64_t CopysetNode::GetConfEpoch() const {
  std::lock_guard<Mutex> lock(confMtx_);
  return epoch_.load(std::memory_order_relaxed);
}

inline std::string CopysetNode::GetCopysetDataDir() const {
  return copysetDataPath_;
}

inline uint64_t CopysetNode::GetAppliedIndex() const {
  return appliedIndex_.load(std::memory_order_acquire);
}

inline void CopysetNode::GetStatus(braft::NodeStatus* status) {
  raftNode_->get_status(status);
}

inline ApplyQueue* CopysetNode::GetApplyQueue() const {
  return applyQueue_.get();
}

inline OperatorMetric* CopysetNode::GetMetric() const { return metric_.get(); }

inline const std::string& CopysetNode::Name() const { return name_; }

inline int64_t CopysetNode::LatestLoadSnapshotIndex() const {
  return latestLoadSnapshotIndex_;
}

inline const braft::PeerId& CopysetNode::GetPeerId() const { return peerId_; }

inline bool CopysetNode::IsLoading() const {
  return isLoading_.load(std::memory_order_acquire);
}

}  // namespace copyset
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_COPYSET_COPYSET_NODE_H_
