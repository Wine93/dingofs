/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2026-04-20
 * Author: AI
 */

#include "cache/infiniband/device.h"

#include <gtest/gtest.h>
#include <infiniband/verbs.h>

namespace dingofs {
namespace cache {
namespace infiniband {

class DeviceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    int num_devices = 0;
    ibv_device** devices = ibv_get_device_list(&num_devices);
    has_hardware_ = (devices != nullptr) && (num_devices > 0);
    if (devices != nullptr) {
      ibv_free_device_list(devices);
    }
  }

  bool has_hardware_ = false;
};

TEST_F(DeviceTest, Open) {
  if (!has_hardware_) {
    GTEST_SKIP() << "no infiniband device available";
  }

  int num_devices = 0;
  ibv_device** devices = ibv_get_device_list(&num_devices);
  ASSERT_NE(devices, nullptr);
  ASSERT_GT(num_devices, 0);

  Device device(devices[0]);
  EXPECT_TRUE(device.Open().ok());

  ibv_free_device_list(devices);
}

class ManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    int num_devices = 0;
    ibv_device** devices = ibv_get_device_list(&num_devices);
    has_hardware_ = (devices != nullptr) && (num_devices > 0);
    if (devices != nullptr) {
      ibv_free_device_list(devices);
    }
  }

  bool has_hardware_ = false;
};

TEST_F(ManagerTest, Start) {
  Manager manager;
  auto status = manager.Start();

  if (has_hardware_) {
    EXPECT_TRUE(status.ok());
  } else {
    EXPECT_FALSE(status.ok());
    EXPECT_TRUE(status.IsInternal());
  }
}

TEST_F(ManagerTest, Shutdown) {
  {
    Manager manager;
    EXPECT_TRUE(manager.Shutdown().ok());
  }

  {
    Manager manager;
    if (has_hardware_) {
      ASSERT_TRUE(manager.Start().ok());
    }
    EXPECT_TRUE(manager.Shutdown().ok());
    EXPECT_TRUE(manager.Shutdown().ok());
  }
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
