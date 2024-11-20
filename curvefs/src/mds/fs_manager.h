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
 * Project: curve
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef CURVEFS_SRC_MDS_FS_MANAGER_H_
#define CURVEFS_SRC_MDS_FS_MANAGER_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "curvefs/proto/mds.pb.h"
#include "curvefs/proto/topology.pb.h"
#include "curvefs/src/mds/common/types.h"
#include "curvefs/src/mds/dlock/dlock.h"
#include "curvefs/src/mds/fs_info_wrapper.h"
#include "curvefs/src/mds/fs_storage.h"
#include "curvefs/src/mds/metaserverclient/metaserver_client.h"
#include "curvefs/src/mds/topology/topology.h"
#include "curvefs/src/mds/topology/topology_manager.h"
#include "curvefs/src/utils/concurrent/concurrent.h"
#include "curvefs/src/utils/interruptible_sleeper.h"
#include "curvefs/src/utils/s3_adapter.h"

namespace curvefs {
namespace mds {

using ::curvefs::mds::Mountpoint;
using ::curvefs::mds::dlock::DLock;
using ::curvefs::mds::topology::PartitionTxId;
using ::curvefs::mds::topology::Topology;
using ::curvefs::mds::topology::TopologyManager;

using ::curvefs::utils::Atomic;
using ::curvefs::utils::InterruptibleSleeper;
using ::curvefs::utils::S3Adapter;
using ::curvefs::utils::Thread;

using curvefs::common::PartitionInfo;

struct FsManagerOption {
  uint32_t backEndThreadRunInterSec;
  uint32_t spaceReloadConcurrency = 10;
  uint32_t clientTimeoutSec = 20;
  curvefs::utils::S3AdapterOption s3AdapterOption;
};

class FsManager {
 public:
  FsManager(const std::shared_ptr<FsStorage>& fs_storage,
            const std::shared_ptr<MetaserverClient>& metaserver_client,
            const std::shared_ptr<TopologyManager>& topo_manager,
            const std::shared_ptr<S3Adapter>& s3_adapter,
            const std::shared_ptr<DLock>& dlock, const FsManagerOption& option)
      : fsStorage_(fs_storage),
        metaserverClient_(metaserver_client),
        topoManager_(topo_manager),
        s3Adapter_(s3_adapter),
        dlock_(dlock),
        isStop_(true),
        option_(option) {}

  bool Init();
  void Run();
  void Stop();
  void Uninit();
  void BackEndFunc();
  void ScanFs(const FsInfoWrapper& wrapper);

  static bool CheckFsName(const std::string& fs_name);

  /**
   * @brief create fs, the fs name can not repeat
   *
   * @param CreateFsRequest request
   * @return FSStatusCode If success return OK; if fsName exist, return
   * FS_EXIST;
   *         else return error code
   */
  FSStatusCode CreateFs(const ::curvefs::mds::CreateFsRequest* request,
                        FsInfo* fs_info);

  /**
   * @brief delete fs, fs must unmount first
   *
   * @param[in] fsName: the fsName of fs which want to delete
   *
   * @return If success return OK; if fs has mount point, return FS_BUSY;
   *         else return error code
   */
  FSStatusCode DeleteFs(const std::string& fs_name);

  /**
   * @brief Mount fs, mount point can not repeat. It will increate
   * mountNum.
   *        If before mount, the mountNum is 0, call spaceClient to
   * InitSpace.
   *
   * @param[in] fsName: fsname of fs
   * @param[in] mountpoint: where the fs mount
   * @param[out] fsInfo: return the fsInfo
   *
   * @return If success return OK;
   *         if fs has same mount point or cto not consistent, return
   *         MOUNT_POINT_CONFLICT; else return error code
   */
  FSStatusCode MountFs(const std::string& fs_name, const Mountpoint& mountpoint,
                       FsInfo* fs_info);

  /**
   * @brief Umount fs, it will decrease mountNum.
   *        If mountNum decrease to zero, call spaceClient to UnInitSpace
   *
   * @param[in] fsName: fsname of fs
   * @param[in] mountpoint: the mountpoint need to umount
   *
   * @return If success return OK;
   *         else return error code
   */
  FSStatusCode UmountFs(const std::string& fs_name,
                        const Mountpoint& mountpoint);

  /**
   * @brief get fs info by fsname
   *
   * @param[in] fsName: the fsname want to get
   * @param[out] fsInfo: the fsInfo got
   *
   * @return If success return OK; else return error code
   */
  FSStatusCode GetFsInfo(const std::string& fs_name, FsInfo* fs_info);

  /**
   * @brief get fs info by fsid
   *
   * @param[in] fsId: the fsid want to get
   * @param[out] fsInfo: the fsInfo got
   *
   * @return If success return OK; else return error code
   */
  FSStatusCode GetFsInfo(uint32_t fs_id, FsInfo* fs_info);

  /**
   * @brief get fs info by fsid and fsname
   *
   * @param[in] fsId: the fsid of fs want to get
   * @param[in] fsName: the fsname of fs want to get
   * @param[out] fsInfo: the fsInfo got
   *
   * @return If success return OK; else return error code
   */
  FSStatusCode GetFsInfo(const std::string& fs_name, uint32_t fs_id,
                         FsInfo* fs_info);

  void GetAllFsInfo(
      ::google::protobuf::RepeatedPtrField<::curvefs::mds::FsInfo>*
          fs_info_vec);

  void RefreshSession(const RefreshSessionRequest* request,
                      RefreshSessionResponse* response);

  void GetLatestTxId(const GetLatestTxIdRequest* request,
                     GetLatestTxIdResponse* response);

  void CommitTx(const CommitTxRequest* request, CommitTxResponse* response);

  // periodically check if the mount point is alive
  void BackEndCheckMountPoint();
  void CheckMountPoint();

  // for utest
  bool GetClientAliveTime(const std::string& mountpoint,
                          std::pair<std::string, uint64_t>* out);

 private:
  // return 0: ExactlySame; 1: uncomplete, -1: neither
  int IsExactlySameOrCreateUnComplete(const std::string& fs_name,
                                      FSType fs_type, uint64_t blocksize,
                                      const FsDetail& detail);

  // send request to metaserver to DeletePartition, if response returns
  // FSStatusCode::OK or FSStatusCode::UNDER_DELETING, returns true;
  // else returns false
  bool DeletePartiton(std::string fs_name, const PartitionInfo& partition);

  // set partition status to DELETING in topology
  bool SetPartitionToDeleting(const PartitionInfo& partition);

  void GetLatestTxId(uint32_t fs_id, std::vector<PartitionTxId>* tx_ids);

  FSStatusCode IncreaseFsTxSequence(const std::string& fs_name,
                                    const std::string& owner,
                                    uint64_t* sequence);

  FSStatusCode GetFsTxSequence(const std::string& fs_name, uint64_t* sequence);

  void UpdateClientAliveTime(const Mountpoint& mountpoint,
                             const std::string& fs_name,
                             bool add_mount_point = true);

  // add mount point to fs if client restore session
  FSStatusCode AddMountPoint(const Mountpoint& mountpoint,
                             const std::string& fs_name);

  void DeleteClientAliveTime(const std::string& mountpoint);

  void RebuildTimeRecorder();

  uint64_t GetRootId();

  std::shared_ptr<FsStorage> fsStorage_;
  std::shared_ptr<MetaserverClient> metaserverClient_;
  curvefs::utils::GenericNameLock<Mutex> nameLock_;
  std::shared_ptr<TopologyManager> topoManager_;
  std::shared_ptr<S3Adapter> s3Adapter_;
  std::shared_ptr<DLock> dlock_;

  // Manage fs background delete threads
  Thread backEndThread_;
  Atomic<bool> isStop_;
  InterruptibleSleeper sleeper_;
  FsManagerOption option_;

  // deal with check mountpoint alive
  Thread checkMountPointThread_;
  InterruptibleSleeper checkMountPointSleeper_;
  // <mountpoint, <fsname,last update time>>
  std::map<std::string, std::pair<std::string, uint64_t>> mpTimeRecorder_;
  mutable RWLock recorderMutex_;
};
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_FS_MANAGER_H_
