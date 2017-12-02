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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger;
import org.apache.hadoop.yarn.server.resourcemanager.RMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerFinishedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

/**
 * Represents an application attempt from the viewpoint of the Fair Scheduler.
 * 每一个FSAppAttempt同时也是一个SchedulerApplicationAttempt实现，即一个被具体的调度器可调度的
 * 对象
 */
@Private
@Unstable
public class FSAppAttempt extends SchedulerApplicationAttempt
    implements Schedulable {

  private static final Log LOG = LogFactory.getLog(FSAppAttempt.class);
  private static final DefaultResourceCalculator RESOURCE_CALCULATOR
      = new DefaultResourceCalculator();

  private long startTime; //StartTime代表的是这个Application的构造时间
  private Priority priority;
  private ResourceWeights resourceWeights;
  private Resource demand = Resources.createResource(0);
  private FairScheduler scheduler;
  private Resource fairShare = Resources.createResource(0, 0);
  private Resource preemptedResources = Resources.createResource(0);
  private RMContainerComparator comparator = new RMContainerComparator();
  private final Map<RMContainer, Long> preemptionMap = new HashMap<RMContainer, Long>();

  /**
   * Delay scheduling: We often want to prioritize scheduling of node-local
   * containers over rack-local or off-switch containers. To achieve this
   * we first only allow node-local assignments for a given priority level,
   * then relax the locality threshold once we've had a long enough period
   * without successfully scheduling. We measure both the number of "missed"
   * scheduling opportunities since the last container was scheduled
   * at the current allowed level and the time since the last container
   * was scheduled. Currently we use only the former.
   */
  private final Map<Priority, NodeType> allowedLocalityLevel =
      new HashMap<Priority, NodeType>();

  public FSAppAttempt(FairScheduler scheduler,
      ApplicationAttemptId applicationAttemptId, String user, FSLeafQueue queue,
      ActiveUsersManager activeUsersManager, RMContext rmContext) {
    super(applicationAttemptId, user, queue, activeUsersManager, rmContext);

    this.scheduler = scheduler;
    this.startTime = scheduler.getClock().getTime();
    this.priority = Priority.newInstance(1);
    this.resourceWeights = new ResourceWeights();
  }

  public ResourceWeights getResourceWeights() {
    return resourceWeights;
  }

  /**
   * Get metrics reference from containing queue.
   */
  public QueueMetrics getMetrics() {
    return queue.getMetrics();
  }

  /**
   * 用于资源释放
   * @param rmContainer
   * @param containerStatus
   * @param event RMContainerEventType.RELEASED
   */
  synchronized public void containerCompleted(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {
    
    Container container = rmContainer.getContainer();
    ContainerId containerId = container.getId();
    
    // Remove from the list of newly allocated containers if found
    newlyAllocatedContainers.remove(rmContainer);
    
    // Inform the container
    //rmContainer 是RM端的container抽象，实现类是RMContainerImpl
    rmContainer.handle(
        new RMContainerFinishedEvent(
            containerId,
            containerStatus, 
            event)
        );
    LOG.info("Completed container: " + rmContainer.getContainerId() + 
        " in state: " + rmContainer.getState() + " event:" + event);
    
    // Remove from the list of containers
    liveContainers.remove(rmContainer.getContainerId());

    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.RELEASE_CONTAINER, "SchedulerApp", 
        getApplicationId(), containerId);
    
    // Update usage metrics 
    //更新队列的metrics信息，将这个释放的container的资源从当前占用资源中去除
    Resource containerResource = rmContainer.getContainer().getResource();
    queue.getMetrics().releaseResources(getUser(), 1, containerResource);
    Resources.subtractFrom(currentConsumption, containerResource);

    // remove from preemption map if it is completed
    preemptionMap.remove(rmContainer);

    // Clear resource utilization metrics cache.
    lastMemoryAggregateAllocationUpdateTime = -1;
  }

  private synchronized void unreserveInternal(
      Priority priority, FSSchedulerNode node) {
    Map<NodeId, RMContainer> reservedContainers = 
        this.reservedContainers.get(priority);
    RMContainer reservedContainer = reservedContainers.remove(node.getNodeID());
    if (reservedContainers.isEmpty()) {
      this.reservedContainers.remove(priority);
    }
    
    // Reset the re-reservation count
    resetReReservations(priority);

    Resource resource = reservedContainer.getContainer().getResource();
    Resources.subtractFrom(currentReservation, resource);

    LOG.info("Application " + getApplicationId() + " unreserved " + " on node "
        + node + ", currently has " + reservedContainers.size() + " at priority "
        + priority + "; currentReservation " + currentReservation);
  }

  /**
   * Headroom depends on resources in the cluster, current usage of the
   * queue, queue's fair-share and queue's max-resources.
   */
  @Override
  public Resource getHeadroom() {
    final FSQueue queue = (FSQueue) this.queue;
    SchedulingPolicy policy = queue.getPolicy();

    Resource queueFairShare = queue.getFairShare();
    Resource queueUsage = queue.getResourceUsage();
    Resource clusterResource = this.scheduler.getClusterResource();
    Resource clusterUsage = this.scheduler.getRootQueueMetrics()
        .getAllocatedResources();

    Resource clusterAvailableResources =
        Resources.subtract(clusterResource, clusterUsage);
    Resource queueMaxAvailableResources =
        Resources.subtract(queue.getMaxShare(), queueUsage);
    Resource maxAvailableResource = Resources.componentwiseMin(
        clusterAvailableResources, queueMaxAvailableResources);

    Resource headroom = policy.getHeadroom(queueFairShare,
        queueUsage, maxAvailableResource);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for " + this.getName() + ":" +
          "Min(" +
          "(queueFairShare=" + queueFairShare +
          " - queueUsage=" + queueUsage + ")," +
          " maxAvailableResource=" + maxAvailableResource +
          "Headroom=" + headroom);
    }
    return headroom;
  }

  public synchronized float getLocalityWaitFactor(
      Priority priority, int clusterNodes) {
    // Estimate: Required unique resources (i.e. hosts + racks)
    int requiredResources = 
        Math.max(this.getResourceRequests(priority).size() - 1, 0);
    
    // waitFactor can't be more than '1' 
    // i.e. no point skipping more than clustersize opportunities
    return Math.min(((float)requiredResources / clusterNodes), 1.0f);
  }

  /**
   * Return the level at which we are allowed to schedule containers, given the
   * current size of the cluster and thresholds indicating how many nodes to
   * fail at (as a fraction of cluster size) before relaxing scheduling
   * constraints.
   */
  public synchronized NodeType getAllowedLocalityLevel(Priority priority,
      int numNodes, double nodeLocalityThreshold, double rackLocalityThreshold) {
    // upper limit on threshold
    if (nodeLocalityThreshold > 1.0) { nodeLocalityThreshold = 1.0; }
    if (rackLocalityThreshold > 1.0) { rackLocalityThreshold = 1.0; }

    // If delay scheduling is not being used, can schedule anywhere
    if (nodeLocalityThreshold < 0.0 || rackLocalityThreshold < 0.0) { //如果我们没有打开延迟调度策略，那么，直接就用OFF_SWITCH
      return NodeType.OFF_SWITCH;
    }
    //如果已经配置了延迟调度，则根据

    // 默认的本地化策略是NODE_LOCAL
    if (!allowedLocalityLevel.containsKey(priority)) { //如果这个优先级目前还没有保存在allowedLocalityLevel，则使用默认的NODE_LOCAL的本地化策略，因为我们总是希望进行NodeLocal的调度
      allowedLocalityLevel.put(priority, NodeType.NODE_LOCAL);
      return NodeType.NODE_LOCAL;
    }

    NodeType allowed = allowedLocalityLevel.get(priority); //如果当前优先级已经有了对应的允许的本地策略略，则根据允许的本地策略

    // If level is already most liberal, we're done
    if (allowed.equals(NodeType.OFF_SWITCH)) return NodeType.OFF_SWITCH; //乳沟直接就允许OFF_SWITCH，就OFF_SWITCH

    double threshold = allowed.equals(NodeType.NODE_LOCAL) ? nodeLocalityThreshold :
      rackLocalityThreshold; //获取允许的本地级别对应的阈值

    // Relax locality constraints once we've surpassed threshold.
    if (getSchedulingOpportunities(priority) > (numNodes * threshold)) { //如果超过了阈值，则需要进行本地化降级
      if (allowed.equals(NodeType.NODE_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.RACK_LOCAL); //将本地化策略从NODE_LOCAL降级为RACK_LOCAL
        resetSchedulingOpportunities(priority);
      }
      else if (allowed.equals(NodeType.RACK_LOCAL)) {
        allowedLocalityLevel.put(priority, NodeType.OFF_SWITCH); //将本地化策略从RACK_LOCAL降级为OFF_SWITCH
        resetSchedulingOpportunities(priority);
      }
    }
    //如果还没有达到阈值，则该是什么本地化就还是什么本地化
    return allowedLocalityLevel.get(priority);
  }

  /**
   * Return the level at which we are allowed to schedule containers.
   * Given the thresholds indicating how much time passed before relaxing
   * scheduling constraints.
   */
  public synchronized NodeType getAllowedLocalityLevelByTime(Priority priority,
          long nodeLocalityDelayMs, long rackLocalityDelayMs,
          long currentTimeMs) {

    //默认NODE_LOCAL和RACK_LOCAL的延迟调度时间是-1，代表我们的FairSchedulerd调度器
    // if not being used, can schedule anywhere
    if (nodeLocalityDelayMs < 0 || rackLocalityDelayMs < 0) { //如果NODE_LOCAL或者RACK_LOCAL允许off-swtich本地级别的调度，则直接返回OFF_SWITCH的本地级别
      return NodeType.OFF_SWITCH;
    }

    //默认的本地级别是NODE_LOCAL
    if (! allowedLocalityLevel.containsKey(priority)) { //
      allowedLocalityLevel.put(priority, NodeType.NODE_LOCAL);
      return NodeType.NODE_LOCAL;
    }

    NodeType allowed = allowedLocalityLevel.get(priority); //获取这个优先级运行的LOCAL级别

    // if level is already most liberal, we're done
    if (allowed.equals(NodeType.OFF_SWITCH)) { //如果这个LOCAL级别直接允许OFF_SWITCH，那就直接OFF_SWITCH
      return NodeType.OFF_SWITCH;
    }

    //如果这个LOCAL级别不允许OFF_SWITCH调度，就需要根据RACK_LOCAL或者NODE_LOCAL以及超时时间进行判断最终是进行NODE_LOCAL、RACK_LOCAL还是OFF_SWITCH的本地级别
    // check waiting time
    long waitTime = currentTimeMs;
    if (lastScheduledContainer.containsKey(priority)) { //如果上一次调度的container还有这个优先级的 ， 则用当前时间减去上一个container的调度时间从而获得等待时间
      waitTime -= lastScheduledContainer.get(priority);
    } else {
      waitTime -= getStartTime();//否则，等待时间就是从这个application启动的时间到现在的时间
    }

    long thresholdTime = allowed.equals(NodeType.NODE_LOCAL) ?
            nodeLocalityDelayMs : rackLocalityDelayMs; //RACK_LOCAL或者NODE_LOCAL等待的阈值

    //如果目前的等待时间已经超过了thresholdTime
    if (waitTime > thresholdTime) { //如果等待时间超过了阈值
      if (allowed.equals(NodeType.NODE_LOCAL)) { //
        allowedLocalityLevel.put(priority, NodeType.RACK_LOCAL);//将NODE_LOCAL降级为RACK_LOCAL
        resetSchedulingOpportunities(priority, currentTimeMs);
      } else if (allowed.equals(NodeType.RACK_LOCAL)) {//将RACK_LOCAL降级为OFF_SWITCH
        allowedLocalityLevel.put(priority, NodeType.OFF_SWITCH);
        resetSchedulingOpportunities(priority, currentTimeMs);
      }
    }
    //如果等待时间还没有超时，那就不会对locality进行降级，原来是什么，现在还是什么
    return allowedLocalityLevel.get(priority);
  }

  synchronized public RMContainer allocate(NodeType type, FSSchedulerNode node,
      Priority priority, ResourceRequest request,
      Container container) {
    // Update allowed locality level
    NodeType allowed = allowedLocalityLevel.get(priority);
    if (allowed != null) {
      if (allowed.equals(NodeType.OFF_SWITCH) &&
          (type.equals(NodeType.NODE_LOCAL) ||
              type.equals(NodeType.RACK_LOCAL))) {
        this.resetAllowedLocalityLevel(priority, type);
      }
      else if (allowed.equals(NodeType.RACK_LOCAL) &&
          type.equals(NodeType.NODE_LOCAL)) {
        this.resetAllowedLocalityLevel(priority, type);
      }
    }

    // Required sanity check - AM can call 'allocate' to update resource 
    // request without locking the scheduler, hence we need to check
    if (getTotalRequiredResources(priority) <= 0) {
      return null;
    }
    
    // Create RMContainer
    RMContainer rmContainer = new RMContainerImpl(container, 
        getApplicationAttemptId(), node.getNodeID(),
        appSchedulingInfo.getUser(), rmContext);

    // Add it to allContainers list.
    newlyAllocatedContainers.add(rmContainer);
    liveContainers.put(container.getId(), rmContainer);    

    // Update consumption and track allocations
    List<ResourceRequest> resourceRequestList = appSchedulingInfo.allocate(
        type, node, priority, request, container);
    Resources.addTo(currentConsumption, container.getResource());

    // Update resource requests related to "request" and store in RMContainer
    ((RMContainerImpl) rmContainer).setResourceRequests(resourceRequestList);

    // Inform the container
    rmContainer.handle(
        new RMContainerEvent(container.getId(), RMContainerEventType.START));

    if (LOG.isDebugEnabled()) {
      LOG.debug("allocate: applicationAttemptId=" 
          + container.getId().getApplicationAttemptId() 
          + " container=" + container.getId() + " host="
          + container.getNodeId().getHost() + " type=" + type);
    }
    RMAuditLogger.logSuccess(getUser(), 
        AuditConstants.ALLOC_CONTAINER, "SchedulerApp", 
        getApplicationId(), container.getId());
    
    return rmContainer;
  }

  /**
   * Should be called when the scheduler assigns a container at a higher
   * degree of locality than the current threshold. Reset the allowed locality
   * level to a higher degree of locality.
   */
  public synchronized void resetAllowedLocalityLevel(Priority priority,
      NodeType level) {
    NodeType old = allowedLocalityLevel.get(priority);
    LOG.info("Raising locality level from " + old + " to " + level + " at " +
        " priority " + priority);
    allowedLocalityLevel.put(priority, level);
  }

  // related methods
  public void addPreemption(RMContainer container, long time) {
    assert preemptionMap.get(container) == null;
    preemptionMap.put(container, time);
    Resources.addTo(preemptedResources, container.getAllocatedResource());
  }

  public Long getContainerPreemptionTime(RMContainer container) {
    return preemptionMap.get(container);
  }

  public Set<RMContainer> getPreemptionContainers() {
    return preemptionMap.keySet();
  }
  
  @Override
  public FSLeafQueue getQueue() {
    return (FSLeafQueue)super.getQueue();
  }

  public Resource getPreemptedResources() {
    return preemptedResources;
  }

  public void resetPreemptedResources() {
    preemptedResources = Resources.createResource(0);
    for (RMContainer container : getPreemptionContainers()) {
      Resources.addTo(preemptedResources, container.getAllocatedResource());
    }
  }

  public void clearPreemptedResources() {
    preemptedResources.setMemory(0);
    preemptedResources.setVirtualCores(0);
  }

  /**
   * Create and return a container object reflecting an allocation for the
   * given appliction on the given node with the given capability and
   * priority.
   */
  public Container createContainer(
      FSSchedulerNode node, Resource capability, Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(
        getApplicationAttemptId(), getNewContainerId());

    // Create the container
    Container container =
        BuilderUtils.newContainer(containerId, nodeId, node.getRMNode()
            .getHttpAddress(), capability, priority, null);

    return container;
  }

  /**
   * Reserve a spot for {@code container} on this {@code node}. If
   * the container is {@code alreadyReserved} on the node, simply
   * update relevant bookeeping. This dispatches ro relevant handlers
   * in {@link FSSchedulerNode}..
   */
  private void reserve(Priority priority, FSSchedulerNode node,
      Container container, boolean alreadyReserved) {
    LOG.info("Making reservation: node=" + node.getNodeName() +
        " app_id=" + getApplicationId());
    //查看FSAppAttempt.assignReservedContainer()，可以看到，对于一个已经进行了reservation的节点，会试图将这个
    //reservation变成allocation,
    if (!alreadyReserved) { //如果这不是一个已经reserve过的attempt，即这个attempt从来都没有做过reservation
      getMetrics().reserveResource(getUser(), container.getResource());
      RMContainer rmContainer =
          super.reserve(node, priority, null, container);
      node.reserveResource(this, priority, rmContainer);//为这个节点进行reserve操作
    } else {
      RMContainer rmContainer = node.getReservedContainer();
      super.reserve(node, priority, rmContainer, container);
      node.reserveResource(this, priority, rmContainer);//如果已经进行了reserve操作，则这里相当于更新一下节点的reserve操作
    }
  }

  /**
   * Remove the reservation on {@code node} at the given {@link Priority}.
   * This dispatches SchedulerNode handlers as well.
   */
  public void unreserve(Priority priority, FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    unreserveInternal(priority, node);
    node.unreserveResource(this);
    getMetrics().unreserveResource(
        getUser(), rmContainer.getContainer().getResource());
  }

  /**
   * Assign a container to this node to facilitate {@code request}. If node does
   * not have enough memory, create a reservation. This is called once we are
   * sure the particular request should be facilitated by this node.
   *
   * @param node
   *     The node to try placing the container on.
   * @param request
   *     The ResourceRequest we're trying to satisfy.
   * @param type
   *     The locality of the assignment.
   * @param reserved
   *     Whether there's already a container reserved for this app on the node.
   * @return
   *     If an assignment was made, returns the resources allocated to the
   *     container.  If a reservation was made, returns
   *     FairScheduler.CONTAINER_RESERVED.  If no assignment or reservation was
   *     made, returns an empty resource.
   */
  private Resource assignContainer(
      FSSchedulerNode node, ResourceRequest request, NodeType type,
      boolean reserved) {

    // How much does this request need?
    Resource capability = request.getCapability(); //请求的资源里那个

    // How much does the node have?
    Resource available = node.getAvailableResource();//这个节点上当前可用的资源

    Container container = null;
    if (reserved) { //如果已经进行了预留，则把这个预留的container分配给它
      container = node.getReservedContainer().getContainer();
    } else {
      container = createContainer(node, capability, request.getPriority()); //在这个节点上创建一个container，来分配个这个应用的请求
    }

    // Can we allocate a container on this node?
    if (Resources.fitsIn(capability, available)) { //如果这个机器的剩余资源能够满足请求的资源量
      // Inform the application of the new container for this request
      RMContainer allocatedContainer = //尽管request中可以携带多个container，但是，每一轮只会分配一个container，下一个container的分配就要等到下一轮 的分配了
          allocate(type, node, request.getPriority(), request, container);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        if (reserved) {
          unreserve(request.getPriority(), node);
        }
        return Resources.none();
      }

      // If we had previously made a reservation, delete it
      if (reserved) {
        unreserve(request.getPriority(), node);
      }

      // Inform the node
      node.allocateContainer(allocatedContainer);

      // If this container is used to run AM, update the leaf queue's AM usage
      if (getLiveContainers().size() == 1 && !getUnmanagedAM()) {
        getQueue().addAMResourceUsage(container.getResource());
        setAmRunning(true);
      }

      return container.getResource();
    } else { //这个机器的剩余资源无法满足请求的资源量
      //判断请求的资源量是否
      if (!FairScheduler.fitsInMaxShare(getQueue(), capability)) {
        return Resources.none();
      }

      // The desired container won't fit here, so reserve
      reserve(request.getPriority(), node, container, reserved);

      return FairScheduler.CONTAINER_RESERVED;
    }
  }

  private boolean hasNodeOrRackLocalRequests(Priority priority) {
    return getResourceRequests(priority).size() > 1;
  }

  private Resource assignContainer(FSSchedulerNode node, boolean reserved) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Node offered to app: " + getName() + " reserved: " + reserved);
    }

    Collection<Priority> prioritiesToTry = (reserved) ?
        Arrays.asList(node.getReservedContainer().getReservedPriority()) :
        getPriorities();

    // For each priority, see if we can schedule a node local, rack local
    // or off-switch request. Rack of off-switch requests may be delayed
    // (not scheduled) in order to promote better locality.
    //对于排序后的Priority集合，我们依次遍历，获取对应Priority对应的container请求，然后，判断这个container
    //是应该进行NODE_LOCAL、RACK_LOCAL还是OFF_SWITCH的本地化策略。对于
    synchronized (this) {
      for (Priority priority : prioritiesToTry) {
        if (getTotalRequiredResources(priority) <= 0 ||
            !hasContainerForNode(priority, node)) { //如果这个priority的资源请求<=0或者这个priority的请求都不适合在这个节点上运行 ，那么直接continue,进入下一个priority判断
          continue;
        }

        addSchedulingOpportunity(priority);

        // Check the AM resource usage for the leaf queue
        //如果liveContainer==0,并且这个Application的AM是一个managedAM，那么在分配container的时候必须考虑
        //是否超过了当前队列的maAMShare配置的最大AM比例值
        if (getLiveContainers().size() == 0 && !getUnmanagedAM()) {
          if (!getQueue().canRunAppAM(getAMResource())) {
            return Resources.none();
          }
        }

        ResourceRequest rackLocalRequest = getResourceRequest(priority,
            node.getRackName()); //获取这个应用请求这个节点所在机架的RACK_LOCAL的请求
        ResourceRequest localRequest = getResourceRequest(priority,
            node.getNodeName()); //获取这个应用请求这个节点的NODE_LOCAL的请求

        if (localRequest != null && !localRequest.getRelaxLocality()) {
          LOG.warn("Relax locality off is not supported on local request: "
              + localRequest);
        }

        NodeType allowedLocality;
        if (scheduler.isContinuousSchedulingEnabled()) { //如果使用的是持续调度，那么需要根据当前的时间确认当前对这个priority可以采取的本地化水平
          allowedLocality = getAllowedLocalityLevelByTime(priority,
              scheduler.getNodeLocalityDelayMs(),
              scheduler.getRackLocalityDelayMs(),
              scheduler.getClock().getTime()); //根据时间去决定允许的本地策略是NODE_LOCAL/RACK_LOCAL/OFF_SWITCH
        } else {
          allowedLocality = getAllowedLocalityLevel(priority,
              scheduler.getNumClusterNodes(),
              scheduler.getNodeLocalityThreshold(), //yarn.scheduler.fair.locality.threshold.node ,这是一个从0到1之间的小数，代表，我必须经过多少次失败的调度，才能允许将本地化策略降级到RACK_LOCAL
              scheduler.getRackLocalityThreshold());//yarn.scheduler.fair.locality.threshold.node,这是一个从0到1之间的小数，代表，我必须经过多少次失败的调度，才能允许将本地化策略降级到OFF_SWITCH
        }

        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && localRequest != null && localRequest.getNumContainers() != 0) { //如果NODE_LOCAL/RACK_LOCAL都不是空的，那么进行NODE_LOCAL级别的调度，因为NODE_LOCAL的优先级最高
          return assignContainer(node, localRequest,
              NodeType.NODE_LOCAL, reserved);
        }

        //准备进行rack_local的调度，但是我们发现rack级别的松弛是false，即不允许松弛到rack,因此无法进行调度了
        if (rackLocalRequest != null && !rackLocalRequest.getRelaxLocality()) {
          continue;
        }
        //rack级别的调度的松弛是true，即允许rack的调度
        if (rackLocalRequest != null && rackLocalRequest.getNumContainers() != 0
            && (allowedLocality.equals(NodeType.RACK_LOCAL) ||
            allowedLocality.equals(NodeType.OFF_SWITCH))) { //如果RACK_LOCAL的请求不是空的并且允许的本地化策略是RACK_LOCAL/OFF_SWITCH，则进行RACK_LOCAL调度
          return assignContainer(node, rackLocalRequest,
              NodeType.RACK_LOCAL, reserved);
        }


        ResourceRequest offSwitchRequest =
            getResourceRequest(priority, ResourceRequest.ANY); //否则，进行OFF_SWITCH调度

        //如果offSwitch的松弛是false，即不允许松弛到off-switch ，只能放弃该priority请求的调度
        if (offSwitchRequest != null && !offSwitchRequest.getRelaxLocality()) {
          continue;
        }

        // 允许进行off-switch的调度
        if (offSwitchRequest != null &&
            offSwitchRequest.getNumContainers() != 0) {
          if (!hasNodeOrRackLocalRequests(priority) ||
              allowedLocality.equals(NodeType.OFF_SWITCH)) {
            return assignContainer(
                node, offSwitchRequest, NodeType.OFF_SWITCH, reserved); //进行OFF_SWITCH调度
          }
        }
      }
    }
    return Resources.none();
  }

  /**
   * Called when this application already has an existing reservation on the
   * given node.  Sees whether we can turn the reservation into an allocation.
   * Also checks whether the application needs the reservation anymore, and
   * releases it if not.
   *
   * @param node
   *     Node that the application has an existing reservation on
   */
  public Resource assignReservedContainer(FSSchedulerNode node) {
    RMContainer rmContainer = node.getReservedContainer();
    Priority priority = rmContainer.getReservedPriority();

    // Make sure the application still needs requests at this priority
    if (getTotalRequiredResources(priority) == 0) {
      unreserve(priority, node);
      return Resources.none();
    }

    // Fail early if the reserved container won't fit.
    // Note that we have an assumption here that there's only one container size
    // per priority.
    if (!Resources.fitsIn(node.getReservedContainer().getReservedResource(),
        node.getAvailableResource())) {
      return Resources.none();
    }

    return assignContainer(node, true);
  }


  /**
   * Whether this app has containers requests that could be satisfied on the
   * given node, if the node had full space.
   * 关于这个方法的判断条件，为什么如果anyRequest==null就直接返回false，这是因为applicationMaster在
   * 为应用申请资源的时候，如果是NODE_LOCAL，顺便也会创建这个节点对应的RACK的RACK_LOCAL的请求和offswitch的请求
   * 这个可以看MRAppMaster发送请求的时候所使用的RMContainerRequestor.addContainerReq()和ApplicationMaster通过
   * 调用 AMRMClientImpl.addContainerRequest()申请资源的过程
   * 或者查看董西成的博客http://dongxicheng.org/mapreduce-nextgen/yarnmrv2-mrappmaster-containerallocator/
   */
  public boolean hasContainerForNode(Priority prio, FSSchedulerNode node) {
    //查找这个优先级下面目前三种请求，一种是没有任何本地化限制的请求，一种是限制为本地机架的请求，一种是限制为本节点内的请求
    ResourceRequest anyRequest = getResourceRequest(prio, ResourceRequest.ANY); //所有请求，对机架和节点没有要求
    ResourceRequest rackRequest = getResourceRequest(prio, node.getRackName());//在这个节点所在机架上的请求
    ResourceRequest nodeRequest = getResourceRequest(prio, node.getNodeName());  //在这个节点上的请求

    return
        // There must be outstanding requests at the given priority:
        anyRequest != null && anyRequest.getNumContainers() > 0 &&
            // If locality relaxation is turned off at *-level, there must be a
            // non-zero request for the node's rack:
            (anyRequest.getRelaxLocality() ||
                (rackRequest != null && rackRequest.getNumContainers() > 0)) &&
            // If locality relaxation is turned off at rack-level, there must be a
            // non-zero request at the node:
            (rackRequest == null || rackRequest.getRelaxLocality() ||
                (nodeRequest != null && nodeRequest.getNumContainers() > 0)) &&
            // The requested container must be able to fit on the node:
            Resources.lessThanOrEqual(RESOURCE_CALCULATOR, null,
                anyRequest.getCapability(), node.getRMNode().getTotalCapability());
  }


  static class RMContainerComparator implements Comparator<RMContainer>,
      Serializable {
    @Override
    public int compare(RMContainer c1, RMContainer c2) {
      int ret = c1.getContainer().getPriority().compareTo(
          c2.getContainer().getPriority());
      if (ret == 0) {
        return c2.getContainerId().compareTo(c1.getContainerId());
      }
      return ret;
    }
  }

  /* Schedulable methods implementation */

  @Override
  public String getName() {
    return getApplicationId().toString();
  }

  @Override
  public Resource getDemand() {
    return demand;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  @Override
  public Resource getMinShare() {
    return Resources.none();
  }

  @Override
  public Resource getMaxShare() {
    return Resources.unbounded();
  }

  @Override
  public Resource getResourceUsage() {
    // Here the getPreemptedResources() always return zero, except in
    // a preemption round
    // In the common case where preempted resource is zero, return the
    // current consumption Resource object directly without calling
    // Resources.subtract which creates a new Resource object for each call.
    return getPreemptedResources().equals(Resources.none()) ?
        getCurrentConsumption() :
        Resources.subtract(getCurrentConsumption(), getPreemptedResources());
  }

  @Override
  public ResourceWeights getWeights() {
    return scheduler.getAppWeight(this);
  }

  @Override
  public Priority getPriority() {
    // Right now per-app priorities are not passed to scheduler,
    // so everyone has the same priority.
    return priority;
  }

  @Override
  public Resource getFairShare() {
    return this.fairShare;
  }

  @Override
  public void setFairShare(Resource fairShare) {
    this.fairShare = fairShare;
  }

  @Override
  public void updateDemand() {
    demand = Resources.createResource(0);
    // Demand is current consumption plus outstanding requests
    Resources.addTo(demand, getCurrentConsumption());

    // Add up outstanding resource requests
    synchronized (this) {
      for (Priority p : getPriorities()) {
        for (ResourceRequest r : getResourceRequests(p).values()) {
          Resource total = Resources.multiply(r.getCapability(), r.getNumContainers());
          Resources.addTo(demand, total);
        }
      }
    }
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    return assignContainer(node, false);
  }

  /**
   * 根据优先级，从application的所有container中选择一个container用来被抢占
   */
  @Override
  public RMContainer preemptContainer() {
	
    if (LOG.isDebugEnabled()) {
      LOG.debug("App " + getName() + " is going to preempt a running " +
          "container");
    }

    RMContainer toBePreempted = null;
    //获取自己所有的running container
    for (RMContainer container : getLiveContainers()) {
    //使用比较器RMContainerComparator选择出一个最应该被抢占的container
      if (!getPreemptionContainers().contains(container) &&
          (toBePreempted == null ||
              comparator.compare(toBePreempted, container) > 0)) {
        toBePreempted = container;
      }
    }
    return toBePreempted;
  }
}
