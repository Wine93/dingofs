/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Created Date: 2023-07-24
 * Author: xuchaojie
 */

#include "src/mds/nameserver2/flatten_core.h"

#include <list>

#include "src/mds/nameserver2/helper/namespace_helper.h"

using curve::mds::chunkserverclient::CopysetClientClosure;
using curve::mds::chunkserverclient::CloneInfo;

namespace curve {
namespace mds {

struct FlattenChunkClosure : public CopysetClientClosure {
    FlattenChunkClosure(const std::shared_ptr<FlattenChunkTaskTracker> &tracker,
        const std::shared_ptr<FlattenChunkContext> &context)
        : tracker_(tracker), context_(context) {}

    void Run() override {
        std::unique_ptr<FlattenChunkClosure> selfGuard(this);
        context_->retCode = GetErrCode();
        if (context_->retCode < 0) {
            LOG(ERROR) << "flatten chunk fail, ret: " << context_->retCode;
        }
        tracker_->PushResultContext(context_);
        tracker_->HandleResponse(context_->retCode);
    }

    std::shared_ptr<FlattenChunkTaskTracker> tracker_;
    std::shared_ptr<FlattenChunkContext> context_;
};

void FlattenCore::DoFlatten(
    const std::string &fileName,
    const FileInfo &fileInfo,
    const FileInfo &snapFileInfo,
    TaskProgress *progress) {
    // 1. 计算克隆的segment数量
    uint64_t segmentSize = fileInfo.segmentsize();
    uint64_t cloneSegmentNum = fileInfo.clonelength() / segmentSize;
    uint64_t chunkNumPerSegment = segmentSize / fileInfo.chunksize();

    int ret = kMdsSuccess;
    uint32_t workingChunkNum = 0;
    auto tracker = std::make_shared<FlattenChunkTaskTracker>();
    for (uint64_t i = 0; i < cloneSegmentNum; i++) {
        uint64_t offset = i * segmentSize;
        progress->SetProgress(100 * i / cloneSegmentNum);

        // 2. 加载克隆的segment
        PageFileSegment segment;
        ret = GetOrAllocCloneSegment(fileName, fileInfo, offset, &segment);
        if (kMdsNotExist == ret) {
            // segment not exist, skip
            ret = kMdsSuccess;
            continue;
        } else if (ret < 0) {
            LOG(ERROR) << "get or alloc clone segment fail, ret: " << ret;
            break;
        }

        if (!segment.has_originfileid()) {
            // not clone segment
            continue;
        }

        // 3. flatten chunk
        LogicalPoolID logicalPoolId = segment.logicalpoolid();
        uint32_t chunkNum = segment.chunks_size();
        for (uint32_t j = 0; j != chunkNum; j++) {
            while (workingChunkNum >= option_.flattenChunkConcurrency) {
                uint32_t completeChunkNum = 0;
                ret = WaitAsycnFlattenChunkDoneAndSendNewPart(
                    tracker, &completeChunkNum);
                workingChunkNum -= completeChunkNum;
                if (ret < 0) {
                    break;
                }
            }
            if (ret < 0) {
                break;
            }

            workingChunkNum++;
            auto context = std::make_shared<FlattenChunkContext>();
            context->fileId = fileInfo.id();
            context->logicalPoolId = logicalPoolId;
            context->copysetId = segment.chunks(j).copysetid();
            context->chunkId = segment.chunks(j).chunkid();
            context->seqNum = fileInfo.seqnum();
            context->chunkSize = fileInfo.chunksize();
            context->partIndex = 0;
            context->partSize = option_.flattenChunkPartSize;
            context->originFileId = segment.originfileid();
            context->chunkIndex = chunkNumPerSegment * i + j;
            context->version = fileInfo.version();
            for (int i = 0; i < fileInfo.clones_size(); i++) {
                CloneInfo cloneInfo;
                cloneInfo.fileId = fileInfo.clones(i).fileid();
                cloneInfo.cloneSn = fileInfo.clones(i).clonesn();
                context->cloneChain.emplace_back(cloneInfo);
            }
            ret = StartAsyncFlattenChunkPart(tracker, context);
            if (ret < 0) {
                break;
            }
        }
        if (ret < 0) {
            break;
        }
    }

    while (workingChunkNum > 0 && ret >= 0) {
        uint32_t completeChunkNum = 0;
        ret = WaitAsycnFlattenChunkDoneAndSendNewPart(
            tracker, &completeChunkNum);
        workingChunkNum -= completeChunkNum;
        if (ret < 0) {
            break;
        }
    }

    if (ret < 0) {
        // still have workingChunkNum when failed, wait all cb return
        if (workingChunkNum > 0) {
            tracker->Wait();
            tracker->PopResultContexts();
        }
        LOG(ERROR) << "flatten file fail, file: " << fileName
                   << ", id: " << fileInfo.id();
        progress->SetStatus(TaskStatus::FAILED);
    } else {
        CHECK(ret >= 0 && workingChunkNum == 0);

        std::string srcFileName = fileInfo.clonesource();
        FileSeqType seq = fileInfo.clonesn();
        FileWriteLockGuard guard(fileLockManager_, fileName, srcFileName);
        // reget FileInfo
        FileInfo fileInfoNew;
        StoreStatus st = storage_->GetFile(
            fileInfo.parentid(), fileInfo.filename(),
            &fileInfoNew);
        if (st != StoreStatus::OK) {
            LOG(ERROR) << "get file info fail, file: " << fileName
                       << ", id: " << fileInfo.id();
            progress->SetStatus(TaskStatus::FAILED);
            return;
        }

        // update file info to storage
        fileInfoNew.set_filestatus(FileStatus::kFileCreated);
        fileInfoNew.set_filetype(FileType::INODE_PAGEFILE);

        // reget snapFileInfo
        FileInfo snapFileInfoNew;
        st = storage_->GetSnapFile(
            snapFileInfo.parentid(), snapFileInfo.filename(),
            &snapFileInfoNew);
        if (st != StoreStatus::OK) {
            LOG(ERROR) << "flatten LookUp SnapFile srcfile: "
                       << snapFileInfo.filename()
                       << ", failed, ret: " << st;
            // not return error, to compatibility
            // with error scenarios
            st = storage_->PutFile(fileInfoNew);
            if (st != StoreStatus::OK) {
                LOG(ERROR) << "update file info fail, file: " << fileName
                           << ", id: " << fileInfo.id();
                progress->SetStatus(TaskStatus::FAILED);
            } else {
                LOG(INFO) << "flatten file success, file: "  << fileName
                          << ", id: "<< fileInfo.id();
                progress->SetStatus(TaskStatus::SUCCESS);
                progress->SetProgress(100);
            }
        } else {
            for (auto it = snapFileInfoNew.mutable_children()->begin();
                it != snapFileInfoNew.mutable_children()->end();
                ++it) {
                if (*it == fileName) {
                    snapFileInfoNew.mutable_children()->erase(it);
                    break;
                }
            }
            if (storage_->Put2File(fileInfoNew, snapFileInfoNew)
                != StoreStatus::OK) {
                LOG(ERROR) << "update file info fail, file: " << fileName
                           << ", id: " << fileInfo.id();
                progress->SetStatus(TaskStatus::FAILED);
            } else {
                LOG(INFO) << "flatten file success, file: "  << fileName
                          << ", id: "<< fileInfo.id();
                progress->SetStatus(TaskStatus::SUCCESS);
                progress->SetProgress(100);
            }
        }
    }
    return;
}

int FlattenCore::GetOrAllocCloneSegment(
    const std::string &fileName,
    const FileInfo &fileInfo,
    uint64_t offset,
    PageFileSegment *segment) {
    // 加锁防止与curvefs中分配segment并发执行
    FileWriteLockGuard guard(fileLockManager_, fileName);
    StoreStatus storeRet = storage_->GetSegment(fileInfo.id(),
        offset, segment);
    if (storeRet == StoreStatus::KeyNotExist) {
        if (fileInfo.clones_size() == 0) {
            LOG(ERROR) << "flatten fileInfo does not have clone chain"
                       << ", file: " << fileName;
            return kMdsFail;
        }
        bool ifCloneSegment = false;
        // lookup clonechain
        for (const auto& cloneInfo : fileInfo.clones()) {
            PageFileSegment originSegment;
            storeRet = storage_->GetSegment(
                cloneInfo.fileid(), offset, &originSegment);
            if (storeRet == StoreStatus::OK) {
                if (originSegment.seqnum() <= cloneInfo.clonesn()) {
                    auto ifok = chunkSegAllocator_->CloneChunkSegment(
                            cloneInfo.fileid(), originSegment, segment);
                    if (ifok == false) {
                        LOG(ERROR) << "CloneChunkSegment error";
                        return kMdsFail;
                    }
                    int64_t revision;
                    if (storage_->PutSegment(fileInfo.id(), offset,
                                             segment, &revision)
                        != StoreStatus::OK) {
                        LOG(ERROR) << "PutSegment fail, fileid = "
                                   << fileInfo.id()
                                   << ", offset = "
                                   << offset;
                        return kMdsFail;
                    }
                    allocStatistic_->AllocSpace(
                        segment->logicalpoolid(),
                        segment->segmentsize(),
                        revision);
                    ifCloneSegment = true;
                    LOG(INFO) << "Flatten clone segment success"
                              << ", fileInfo.id() = "
                              << fileInfo.id()
                              << ", offset = " << offset;
                    break;
                } else {
                    // the origin segment is new allocated,
                    // user normal allocation
                    ifCloneSegment = false;
                    break;
                }
            } else if (storeRet == StoreStatus::KeyNotExist) {
                continue;
            } else {
                LOG(ERROR) << "GetSegment error, fileid = "
                           << cloneInfo.fileid()
                           << ", offset = " << offset
                           << ", storeRet = " << storeRet;
                return kMdsFail;
            }
        }
        if (!ifCloneSegment) {
            return kMdsNotExist;
        }
    } else if (storeRet != StoreStatus::OK) {
        LOG(ERROR) << "load clone segment fail, file: " << fileName
                   << ", id: " << fileInfo.id()
                   << ", offset: " << offset;
        return kMdsFail;
    }
    return kMdsSuccess;
}

int FlattenCore::StartAsyncFlattenChunkPart(
    const std::shared_ptr<FlattenChunkTaskTracker> &tracker,
    const std::shared_ptr<FlattenChunkContext> &context) {

    std::unique_ptr<FlattenChunkClosure> cb(
        new FlattenChunkClosure(tracker, context));
    int ret = copysetClient_->FlattenChunk(context, cb.get());
    if (ret < 0) {
        LOG(ERROR) << "Start flatten chunk fail, ret: " << ret;
        return ret;
    }
    cb.release();
    tracker->AddOneTrace();
    return ret;
}

int FlattenCore::WaitAsycnFlattenChunkDoneAndSendNewPart(
    const std::shared_ptr<FlattenChunkTaskTracker> &tracker,
    uint32_t *completeChunkNum) {
    *completeChunkNum = 0;
    tracker->WaitSome(1);
    std::list<std::shared_ptr<FlattenChunkContext>> results =
        tracker->PopResultContexts();
    for (auto &context : results) {
        if (context->retCode < 0) {
            LOG(ERROR) << "WaitAsycnFlattenChunkDone get result fail"
                       << ", ret: " << context->retCode;
            return context->retCode;
        } else {
            context->partIndex++;
            if (context->partIndex * option_.flattenChunkPartSize >=
                    context->chunkSize) {
                (*completeChunkNum)++;
                continue;
            }

            if ((context->partIndex + 1) * option_.flattenChunkPartSize >
                    context->chunkSize) {
                context->partSize = context->chunkSize -
                    context->partIndex * option_.flattenChunkPartSize;
            } else {
                context->partSize = option_.flattenChunkPartSize;
            }

            int ret = StartAsyncFlattenChunkPart(tracker, context);
            if (ret < 0) {
                LOG(ERROR) << "StartAsyncFlattenChunk fail, ret: " << ret;
                return ret;
            }
        }
    }
    return kMdsSuccess;
}


}  // namespace mds
}  // namespace curve

