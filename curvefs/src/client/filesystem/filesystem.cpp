/*
 *  Copyright (c) 2023 NetEase Inc.
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
 * Project: Curve
 * Created Date: 2023-03-08
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/filesystem/filesystem.h"

#include <cstdint>
#include <memory>

#include "curvefs/src/base/timer/timer_impl.h"
#include "curvefs/src/client/common/dynamic_config.h"
#include "curvefs/src/client/filesystem/dir_cache.h"
#include "curvefs/src/client/filesystem/dir_parent_watcher.h"
#include "curvefs/src/client/filesystem/fs_stat_manager.h"
#include "curvefs/src/client/filesystem/utils.h"

namespace curvefs {
namespace client {
namespace filesystem {

using base::timer::TimerImpl;

USING_FLAG(stat_timer_thread_num);

FileSystem::FileSystem(uint32_t fs_id, FileSystemOption option,
                       ExternalMember member)
    : fs_id_(fs_id), option_(option), member(member) {
  deferSync_ = std::make_shared<DeferSync>(option.deferSyncOption);
  negative_ = std::make_shared<LookupCache>(option.lookupCacheOption);
  dirCache_ = std::make_shared<DirCache>(option.dirCacheOption);
  openFiles_ = std::make_shared<OpenFiles>(option_.openFilesOption, deferSync_);
  attrWatcher_ = std::make_shared<AttrWatcher>(option_.attrWatcherOption,
                                               openFiles_, dirCache_);
  entry_watcher_ = std::make_shared<EntryWatcher>(option_.nocto_suffix);
  handlerManager_ = std::make_shared<HandlerManager>();
  rpc_ = std::make_shared<RPCClient>(option.rpcOption, member);
}

void FileSystem::Run() {
  deferSync_->Start();
  dirCache_->Start();

  dir_parent_watcher_ =
      std::make_shared<DirParentWatcherImpl>(member.inodeManager);

  stat_timer_ =
      std::make_shared<base::timer::TimerImpl>(FLAGS_stat_timer_thread_num);
  fs_stat_manager_ =
      std::make_shared<FsStatManager>(fs_id_, member.meta_client, stat_timer_);
  dir_quota_manager_ = std::make_shared<DirQuotaManager>(
      fs_id_, member.meta_client, dir_parent_watcher_, stat_timer_);

  // must start before fs_stat_manager_ and dir_quota_manager_
  stat_timer_->Start();

  fs_stat_manager_->Start();
  dir_quota_manager_->Start();
}

void FileSystem::Destory() {
  openFiles_->CloseAll();
  deferSync_->Stop();
  dirCache_->Stop();

  stat_timer_->Stop();
  fs_stat_manager_->Stop();
  dir_quota_manager_->Stop();
}

void FileSystem::Attr2Stat(InodeAttr* attr, struct stat* stat) {
  std::memset(stat, 0, sizeof(struct stat));
  stat->st_ino = attr->inodeid();        //  inode number
  stat->st_mode = attr->mode();          // permission mode
  stat->st_nlink = attr->nlink();        // number of links
  stat->st_uid = attr->uid();            // user ID of owner
  stat->st_gid = attr->gid();            // group ID of owner
  stat->st_size = attr->length();        // total size, in bytes
  stat->st_rdev = attr->rdev();          // device ID (if special file)
  stat->st_atim.tv_sec = attr->atime();  // time of last access
  stat->st_atim.tv_nsec = attr->atime_ns();
  stat->st_mtim.tv_sec = attr->mtime();  // time of last modification
  stat->st_mtim.tv_nsec = attr->mtime_ns();
  stat->st_ctim.tv_sec = attr->ctime();  // time of last status change
  stat->st_ctim.tv_nsec = attr->ctime_ns();
  stat->st_blksize = option_.blockSize;  // blocksize for file system I/O
  stat->st_blocks = 0;                   // number of 512B blocks allocated
  if (IsS3File(*attr)) {
    stat->st_blocks = (attr->length() + 511) / 512;
  }
}

void FileSystem::Entry2Param(EntryOut* entry_out, fuse_entry_param* e) {
  std::memset(e, 0, sizeof(fuse_entry_param));
  e->ino = entry_out->attr.inodeid();
  e->generation = 0;
  Attr2Stat(&entry_out->attr, &e->attr);
  e->entry_timeout = entry_out->entryTimeout;
  e->attr_timeout = entry_out->attrTimeout;
}

void FileSystem::SetEntryTimeout(EntryOut* entry_out) {
  auto option = option_.kernelCacheOption;
  if (IsDir(entry_out->attr)) {
    entry_out->entryTimeout = option.dirEntryTimeoutSec;
    entry_out->attrTimeout = option.dirAttrTimeoutSec;
  } else {
    entry_out->entryTimeout = option.entryTimeoutSec;
    entry_out->attrTimeout = option.attrTimeoutSec;
  }
}

void FileSystem::SetAttrTimeout(AttrOut* attr_out) {
  auto option = option_.kernelCacheOption;
  if (IsDir(attr_out->attr)) {
    attr_out->attrTimeout = option.dirAttrTimeoutSec;
  } else {
    attr_out->attrTimeout = option.attrTimeoutSec;
  }
}

// fuse reply*
void FileSystem::ReplyError(Request req, CURVEFS_ERROR code) {
  fuse_reply_err(req, SysErr(code));
}

void FileSystem::ReplyEntry(Request req, EntryOut* entry_out) {
  AttrWatcherGuard watcher(attrWatcher_, &entry_out->attr, ReplyType::ATTR,
                           true);
  DirParentWatcherGuard parent_watcher(dir_parent_watcher_, entry_out->attr);

  fuse_entry_param e;
  SetEntryTimeout(entry_out);
  Entry2Param(entry_out, &e);
  fuse_reply_entry(req, &e);
}

void FileSystem::ReplyAttr(Request req, AttrOut* attr_out) {
  AttrWatcherGuard watcher(attrWatcher_, &attr_out->attr, ReplyType::ATTR,
                           true);
  struct stat stat;
  SetAttrTimeout(attr_out);
  Attr2Stat(&attr_out->attr, &stat);
  fuse_reply_attr(req, &stat, attr_out->attrTimeout);
}

void FileSystem::ReplyReadlink(Request req, const std::string& link) {
  fuse_reply_readlink(req, link.c_str());
}

void FileSystem::ReplyOpen(Request req, FileInfo* fi) {
  fuse_reply_open(req, fi);
}

void FileSystem::ReplyOpen(Request req, FileOut* file_out) {
  AttrWatcherGuard watcher(attrWatcher_, &file_out->attr,
                           ReplyType::ONLY_LENGTH, true);
  fuse_reply_open(req, file_out->fi);
}

void FileSystem::ReplyData(Request req, struct fuse_bufvec* bufv,
                           enum fuse_buf_copy_flags flags) {
  fuse_reply_data(req, bufv, flags);
}

void FileSystem::ReplyWrite(Request req, FileOut* file_out) {
  AttrWatcherGuard watcher(attrWatcher_, &file_out->attr,
                           ReplyType::ONLY_LENGTH, true);
  fuse_reply_write(req, file_out->nwritten);
}

void FileSystem::ReplyBuffer(Request req, const char* buf, size_t size) {
  fuse_reply_buf(req, buf, size);
}

void FileSystem::ReplyStatfs(Request req, const struct statvfs* stbuf) {
  fuse_reply_statfs(req, stbuf);
}

void FileSystem::ReplyXattr(Request req, size_t size) {
  fuse_reply_xattr(req, size);
}

void FileSystem::ReplyCreate(Request req, EntryOut* entry_out, FileInfo* fi) {
  AttrWatcherGuard watcher(attrWatcher_, &entry_out->attr, ReplyType::ATTR,
                           true);
  fuse_entry_param e;
  SetEntryTimeout(entry_out);
  Entry2Param(entry_out, &e);
  fuse_reply_create(req, &e, fi);
}

void FileSystem::AddDirEntry(Request req, DirBufferHead* buffer,
                             DirEntry* dir_entry) {
  struct stat stat;
  std::memset(&stat, 0, sizeof(stat));
  stat.st_ino = dir_entry->ino;

  // add a directory entry to the buffer
  size_t oldsize = buffer->size;
  const char* name = dir_entry->name.c_str();
  buffer->size += fuse_add_direntry(req, NULL, 0, name, NULL, 0);
  buffer->p = static_cast<char*>(realloc(buffer->p, buffer->size));
  fuse_add_direntry(req,
                    buffer->p + oldsize,     // char* buf
                    buffer->size - oldsize,  // size_t bufisze
                    name, &stat, buffer->size);
}

void FileSystem::AddDirEntryPlus(Request req, DirBufferHead* buffer,
                                 DirEntry* dir_entry) {
  AttrWatcherGuard watcher(attrWatcher_, &dir_entry->attr, ReplyType::ATTR,
                           false);
  struct fuse_entry_param e;
  EntryOut entryOut(dir_entry->attr);
  SetEntryTimeout(&entryOut);
  Entry2Param(&entryOut, &e);

  // add a directory entry to the buffer with the attributes
  size_t oldsize = buffer->size;
  const char* name = dir_entry->name.c_str();
  buffer->size += fuse_add_direntry_plus(req, NULL, 0, name, NULL, 0);
  buffer->p = static_cast<char*>(realloc(buffer->p, buffer->size));
  fuse_add_direntry_plus(req,
                         buffer->p + oldsize,     // char* buf
                         buffer->size - oldsize,  // size_t bufisze
                         name, &e, buffer->size);
}

// handler*
std::shared_ptr<FileHandler> FileSystem::NewHandler() {
  return handlerManager_->NewHandler();
}

std::shared_ptr<FileHandler> FileSystem::FindHandler(uint64_t fh) {
  return handlerManager_->FindHandler(fh);
}

void FileSystem::ReleaseHandler(uint64_t fh) {
  return handlerManager_->ReleaseHandler(fh);
}

FileSystemMember FileSystem::BorrowMember() {
  return FileSystemMember(deferSync_, openFiles_, attrWatcher_, entry_watcher_);
}

// fuse request*
CURVEFS_ERROR FileSystem::Lookup(Request req, Ino parent,
                                 const std::string& name, EntryOut* entry_out) {
  if (name.size() > option_.maxNameLength) {
    return CURVEFS_ERROR::NAMETOOLONG;
  }

  bool yes = negative_->Get(parent, name);
  if (yes) {
    return CURVEFS_ERROR::NOTEXIST;
  }

  auto rc = rpc_->Lookup(parent, name, entry_out);
  if (rc == CURVEFS_ERROR::OK) {
    negative_->Delete(parent, name);
  } else if (rc == CURVEFS_ERROR::NOTEXIST) {
    negative_->Put(parent, name);
  }
  return rc;
}

CURVEFS_ERROR FileSystem::GetAttr(Request req, Ino ino, AttrOut* attr_out) {
  InodeAttr attr;
  auto rc = rpc_->GetAttr(ino, &attr);
  if (rc == CURVEFS_ERROR::OK) {
    *attr_out = AttrOut(attr);
  }
  return rc;
}

CURVEFS_ERROR FileSystem::OpenDir(Request req, Ino ino, FileInfo* fi) {
  InodeAttr attr;
  CURVEFS_ERROR rc = rpc_->GetAttr(ino, &attr);
  if (rc != CURVEFS_ERROR::OK) {
    return rc;
  }

  // revalidate directory cache
  std::shared_ptr<DirEntryList> entries;
  bool yes = dirCache_->Get(ino, &entries);
  if (yes) {
    if (entries->GetMtime() != AttrMtime(attr)) {
      dirCache_->Drop(ino);
    }
  }

  auto handler = NewHandler();
  handler->mtime = AttrMtime(attr);
  fi->fh = handler->fh;
  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::ReadDir(Request req, Ino ino, FileInfo* fi,
                                  std::shared_ptr<DirEntryList>* entries) {
  bool yes = dirCache_->Get(ino, entries);
  if (yes) {
    return CURVEFS_ERROR::OK;
  }

  CURVEFS_ERROR rc = rpc_->ReadDir(ino, entries);
  if (rc != CURVEFS_ERROR::OK) {
    return rc;
  }

  (*entries)->SetMtime(FindHandler(fi->fh)->mtime);
  dirCache_->Put(ino, *entries);
  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::ReleaseDir(Request req, Ino ino, FileInfo* fi) {
  ReleaseHandler(fi->fh);
  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::Open(Request req, Ino ino, FileInfo* fi) {
  std::shared_ptr<InodeWrapper> inode;
  bool yes = openFiles_->IsOpened(ino, &inode);
  if (yes) {
    openFiles_->Open(ino, inode);
    // fi->keep_cache = 1;
    return CURVEFS_ERROR::OK;
  }

  CURVEFS_ERROR rc = rpc_->Open(ino, &inode);
  if (rc != CURVEFS_ERROR::OK) {
    return rc;
  }

  TimeSpec mtime;
  yes = attrWatcher_->GetMtime(ino, &mtime);
  if (!yes) {
    // It is rare which only arise when attribute evited for attr-watcher.
    LOG(WARNING) << "open(" << ino << "): stale file handler"
                 << ": attribute not found in wacther";
    return CURVEFS_ERROR::STALE;
  } else if (mtime != InodeMtime(inode)) {
    LOG(WARNING) << "open(" << ino << "): stale file handler" << ", cache("
                 << mtime << ") vs remote(" << InodeMtime(inode) << ")";
    return CURVEFS_ERROR::STALE;
  }

  openFiles_->Open(ino, inode);
  return CURVEFS_ERROR::OK;
}

CURVEFS_ERROR FileSystem::Release(Request req, Ino ino, FileInfo* fi) {
  if (ino == STATSINODEID) {
    ReleaseHandler(fi->fh);
    return CURVEFS_ERROR::OK;
  }
  openFiles_->Close(ino);
  return CURVEFS_ERROR::OK;
}

void FileSystem::UpdateFsQuotaUsage(int64_t add_space, int64_t add_inode) {
  fs_stat_manager_->UpdateQuotaUsage(add_space, add_inode);
}

void FileSystem::UpdateDirQuotaUsage(Ino ino, int64_t add_space,
                                     int64_t add_inode) {
  dir_quota_manager_->UpdateDirQuotaUsage(ino, add_space, add_inode);
}

bool FileSystem::CheckFsQuota(int64_t add_space, int64_t add_inode) {
  return fs_stat_manager_->CheckQuota(add_space, add_inode);
}

bool FileSystem::CheckDirQuota(Ino ino, int64_t add_space, int64_t add_inode) {
  return dir_quota_manager_->CheckDirQuota(ino, add_space, add_inode);
}

bool FileSystem::CheckQuota(Ino ino, int64_t add_space, int64_t add_inode) {
  if (!fs_stat_manager_->CheckQuota(add_space, add_inode)) {
    return false;
  }

  return dir_quota_manager_->CheckDirQuota(ino, add_space, add_inode);
}

bool FileSystem::NearestDirQuota(Ino ino, Ino& out_quota_ino) {
  return dir_quota_manager_->NearestDirQuota(ino, out_quota_ino);
}

Quota FileSystem::GetFsQuota() { return fs_stat_manager_->GetQuota(); }

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs
