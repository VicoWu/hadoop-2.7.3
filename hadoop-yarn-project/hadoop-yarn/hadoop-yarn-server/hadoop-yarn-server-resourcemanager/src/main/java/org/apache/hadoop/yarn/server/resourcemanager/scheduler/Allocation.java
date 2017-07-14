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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.List;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;

/**
 * Scheduler通过处理资源请求等信息得出的资源分配信息，关于strictContainers和fungibleContainers，可以看PreemptionMessage.java
 * @author wuchang
 *
 */
public class Allocation {
	//分配给对应ApplicationMaster的资源
  final List<Container> containers;
//严格抢占的container,这些container当前还没有被抢占，只是标记为被强占，这些container在真正被强占前通过response告知AM
  //方便AM保存现场等操作，一旦超过指定时间，这些container就会被抢占和回收
  final Set<ContainerId> strictContainers;
  //灵活抢占的container，这些container也被标记为被抢占，但是，还有回旋余地，如果fungibleResources中描述的资源请求能够
  //被满足，则fungibleContainers不会被抢占
  final Set<ContainerId> fungibleContainers;
  //如果fungibleResources中描述的资源能够被这个AppliationMaster返回给ResourceManager，那么fungibleContainers就不会被抢占
  final List<ResourceRequest> fungibleResources;
  //NodeManager的token信息，相当于RM授权对应的AM访问对应的节点以创建container等
  final List<NMToken> nmTokens;
  //资源限制，理解为真正可以被这个ApplicationMaster使用的剩余资源
  private Resource resourceLimit;

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources) {
    this(containers,  resourceLimit,strictContainers,  fungibleContainers,
      fungibleResources, null);
  }

  public Allocation(List<Container> containers, Resource resourceLimit,
      Set<ContainerId> strictContainers, Set<ContainerId> fungibleContainers,
      List<ResourceRequest> fungibleResources, List<NMToken> nmTokens) {
    this.containers = containers;
    this.resourceLimit = resourceLimit;
    this.strictContainers = strictContainers;
    this.fungibleContainers = fungibleContainers;
    this.fungibleResources = fungibleResources;
    this.nmTokens = nmTokens;
  }

  public List<Container> getContainers() {
    return containers;
  }

  public Resource getResourceLimit() {
    return resourceLimit;
  }

  public Set<ContainerId> getStrictContainerPreemptions() {
    return strictContainers;
  }

  public Set<ContainerId> getContainerPreemptions() {
    return fungibleContainers;
  }

  public List<ResourceRequest> getResourcePreemptions() {
    return fungibleResources;
  }
    
  public List<NMToken> getNMTokens() {
    return nmTokens;
  }

  @VisibleForTesting
  public void setResourceLimit(Resource resource) {
    this.resourceLimit = resource;
  }
}
