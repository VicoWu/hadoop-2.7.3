/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Given a set of checkable resources, this class is capable of determining
 * whether sufficient resources are available for the NN to continue operating.
 */
@InterfaceAudience.Private
final class NameNodeResourcePolicy {

  /**
   * Return true if and only if there are sufficient NN
   * resources to continue logging edits.
   * 
   * @param resources the collection of resources to check.
   * @param minimumRedundantResources the minimum number of redundant resources
   *        required to continue operation.
   * @return true if and only if there are sufficient NN resources to
   *         continue logging edits.
   */
  static boolean areResourcesAvailable(
      Collection<? extends CheckableNameNodeResource> resources,
      int minimumRedundantResources) {

    // TODO: workaround:
    // - during startup, if there are no edits dirs on disk, then there is
    // a call to areResourcesAvailable() with no dirs at all, which was
    // previously causing the NN to enter safemode
    if (resources.isEmpty()) {
      return true;
    }
    
    int requiredResourceCount = 0;
    int redundantResourceCount = 0;
    int disabledRedundantResourceCount = 0;
    //这里的每一个CheckableNameNodeResource接口的实现类是JournalAndStream对象
    for (CheckableNameNodeResource resource : resources) {
      if (!resource.isRequired()) {//CheckableNameNodeResource中的isRequired=true代表只有当这个资源是available，NN才能继续操作
        redundantResourceCount++;//这个资源不是必须的，是冗余资源
        if (!resource.isResourceAvailable()) {//如果这个冗余资源不可用
          disabledRedundantResourceCount++;//对不可用的冗余资源计数
        }
      } else {
        requiredResourceCount++;//这个资源是必须的
        if (!resource.isResourceAvailable()) {
          // Short circuit - a required resource is not available.
          return false;//只要有一个requiredResource处于unavailable状态，则返回false
        }
      }
    }
    //所有的必须资源都处于available状态，继续进行下面的判断
    if (redundantResourceCount == 0) {//如果没有任何一个非必须资源
      // If there are no redundant resources, return true if there are any
      // required resources available.
      return requiredResourceCount > 0;//判断
    } else {//存在非必须资源
      return redundantResourceCount - disabledRedundantResourceCount >=
          minimumRedundantResources;//满足最小可用的冗余资源配置
    }
  }
}
