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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.policies;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;

/**
 * Contains logic for computing the fair shares. A {@link Schedulable}'s fair
 * share is {@link Resource} it is entitled to, independent of the current
 * demands and allocations on the cluster. A {@link Schedulable} whose resource
 * consumption lies at or below its fair share will never have its containers
 * preempted.
 */
public class ComputeFairShares {
  
  private static final int COMPUTE_FAIR_SHARES_ITERATIONS = 25;

  /**
   * Compute fair share of the given schedulables.Fair share is an allocation of
   * shares considering only active schedulables ie schedulables which have
   * running apps.
   * 
   * @param schedulables
   * @param totalResources
   * @param type
   */
  public static void computeShares(
      Collection<? extends Schedulable> schedulables, Resource totalResources,
      ResourceType type) {
    computeSharesInternal(schedulables, totalResources, type, false);
  }

  /**
   * Compute the steady fair share of the given queues. The steady fair
   * share is an allocation of shares considering all queues, i.e.,
   * active and inactive.
   *
   * @param queues
   * @param totalResources
   * @param type
   */
  public static void computeSteadyShares(
      Collection<? extends FSQueue> queues, Resource totalResources,
      ResourceType type) {
    computeSharesInternal(queues, totalResources, type, true);
  }

  /**
   * Given a set of Schedulables and a number of slots, compute their weighted
   * fair shares. The min and max shares and of the Schedulables are assumed to
   * be set beforehand. We compute the fairest possible allocation of shares to
   * the Schedulables that respects their min and max shares.
   * <p>
   * To understand what this method does, we must first define what weighted
   * fair sharing means in the presence of min and max shares. If there
   * were no minimum or maximum shares, then weighted fair sharing would be
   * achieved if the ratio of slotsAssigned / weight was equal for each
   * Schedulable and all slots were assigned. Minimum and maximum shares add a
   * further twist - Some Schedulables may have a min share higher than their
   * assigned share or a max share lower than their assigned share.
   * <p>
   * To deal with these possibilities, we define an assignment of slots as being
   * fair if there exists a ratio R such that: Schedulables S where S.minShare
   * {@literal >} R * S.weight are given share S.minShare - Schedulables S
   * where S.maxShare {@literal <} R * S.weight are given S.maxShare -
   * All other Schedulables S are assigned share R * S.weight -
   * The sum of all the shares is totalSlots.
   * <p>
   * We call R the weight-to-slots ratio because it converts a Schedulable's
   * weight to the number of slots it is assigned.
   * <p>
   * We compute a fair allocation by finding a suitable weight-to-slot ratio R.
   * To do this, we use binary search. Given a ratio R, we compute the number of
   * slots that would be used in total with this ratio (the sum of the shares
   * computed using the conditions above). If this number of slots is less than
   * totalSlots, then R is too small and more slots could be assigned. If the
   * number of slots is more than totalSlots, then R is too large.
   * <p>
   * We begin the binary search with a lower bound on R of 0 (which means that
   * all Schedulables are only given their minShare) and an upper bound computed
   * to be large enough that too many slots are given (by doubling R until we
   * use more than totalResources resources). The helper method
   * resourceUsedWithWeightToResourceRatio computes the total resources used with a
   * given value of R.
   * <p>
   * The running time of this algorithm is linear in the number of Schedulables,
   * because resourceUsedWithWeightToResourceRatio is linear-time and the number of
   * iterations of binary search is a constant (dependent on desired precision).
   */
  private static void computeSharesInternal(
      Collection<? extends Schedulable> allSchedulables,
      Resource totalResources, ResourceType type, boolean isSteadyShare) {

    Collection<Schedulable> schedulables = new ArrayList<Schedulable>();
    //所有计算完毕准备取走的resource
    int takenResources = handleFixedFairShares(
        allSchedulables, schedulables, isSteadyShare, type);

    if (schedulables.isEmpty()) {
      return;
    }
    // Find an upper bound on R that we can use in our binary search. We start
    // at R = 1 and double it until we have either used all the resources or we
    // have met all Schedulables' max shares.
    int totalMaxShare = 0;//non-fix队列的所有max-share之和
    for (Schedulable sched : schedulables) {//对于non-fixed队列，计算maxShare之和
      int maxShare = getResourceValue(sched.getMaxShare(), type);
      totalMaxShare = (int) Math.min((long)maxShare + (long)totalMaxShare,
          Integer.MAX_VALUE);
      if (totalMaxShare == Integer.MAX_VALUE) {
        break;
      }
    }

    //集群总资源，减去 已经计算完毕的fix队列的资源，得到剩下的non-fix的资源总量
    int totalResource = Math.max((getResourceValue(totalResources, type) -
        takenResources), 0);
    //所有non-fix队列的maxShare加起来小于 totalResource（集群总资源减去fix队列资源量的和的剩余值）,则只需要所有maxShare的和就可以了，否则，需要totalResuorce（集群总资源减去fix队列资源量的和的剩余值）
    totalResource = Math.min(totalMaxShare, totalResource);

    double rMax = 1.0;
    while (resourceUsedWithWeightToResourceRatio(rMax, schedulables, type)
        < totalResource) {
      rMax *= 2.0;
    }
    //获取了一个最大值，可以在0和这个最大值之间进行二分查找了。二分查找结束以后，right 值就几乎逼近了non fix队列的可用资源值
    // Perform the binary search for up to COMPUTE_FAIR_SHARES_ITERATIONS steps
    double left = 0;
    double right = rMax;
    for (int i = 0; i < COMPUTE_FAIR_SHARES_ITERATIONS; i++) {
      double mid = (left + right) / 2.0;
      int plannedResourceUsed = resourceUsedWithWeightToResourceRatio(
          mid, schedulables, type);
      if (plannedResourceUsed == totalResource) {
        right = mid;
        break;
      } else if (plannedResourceUsed < totalResource) {
        left = mid;
      } else {
        right = mid;
      }
    }
    //二分查找完毕，right中存放了正确的R值
    // Set the fair shares based on the value of R we've converged to
    for (Schedulable sched : schedulables) {
      if (isSteadyShare) {//如果是计算steady fair share , 则设置这个steady fair share值
        setResourceValue(computeShare(sched, right, type),
            ((FSQueue) sched).getSteadyFairShare(), type);//根据全局的right值设置这个队列的steady fair share 值
      } else {
        setResourceValue(//否则，//根据全局的right值设置这个队列的 fair share 值
            computeShare(sched, right, type), sched.getFairShare(), type);
      }
    }
  }

  /**
   * Compute the resources that would be used given a weight-to-resource ratio
   * w2rRatio, for use in the computeFairShares algorithm as described in #
   */
  private static int resourceUsedWithWeightToResourceRatio(double w2rRatio,
      Collection<? extends Schedulable> schedulables, ResourceType type) {
    int resourcesTaken = 0;
    for (Schedulable sched : schedulables) {
      int share = computeShare(sched, w2rRatio, type);
      resourcesTaken += share;
    }
    return resourcesTaken;
  }

  /**
   * Compute the resources assigned to a Schedulable given a particular
   * weight-to-resource ratio w2rRatio.
   * 如果sched.getWeights().getWeight(type) * w2rRatio;介于minShare 和 maxShare之间，则直接返回sched.getWeights().getWeight(type) * w2rRatio;，
   * 否则，如果sched.getWeights().getWeight(type) * w2rRatio;小于minShare , 则使用minShare
   * 如果sched.getWeights().getWeight(type) * w2rRatio  > maxShare, 则使用maxShare
   */
  private static int computeShare(Schedulable sched, double w2rRatio,
      ResourceType type) {
    double share = sched.getWeights().getWeight(type) * w2rRatio;
    share = Math.max(share, getResourceValue(sched.getMinShare(), type));//取share和minShare中的较大值
    share = Math.min(share, getResourceValue(sched.getMaxShare(), type));//取share和maxShare中的较小值
    return (int) share;
  }

  /**
   * Helper method to handle Schedulabes with fixed fairshares.
   * Returns the resources taken by fixed fairshare schedulables,
   * and adds the remaining to the passed nonFixedSchedulables.
   */
  private static int handleFixedFairShares(
      Collection<? extends Schedulable> schedulables,
      Collection<Schedulable> nonFixedSchedulables,
      boolean isSteadyShare, ResourceType type) {
    int totalResource = 0;//所有队列的总资源求和

    for (Schedulable sched : schedulables) {
      //如果是一个fixed队列，则fixedShare为该队列的minShare或者0
      int fixedShare = getFairShareIfFixed(sched, isSteadyShare, type);
      if (fixedShare < 0) {//不是fixed，即maxShare 和weight不是0， 并且这个Schedulable是一个active的
        nonFixedSchedulables.add(sched);//将nonFixed队列放入nonFixedSchedulables中返回
      } else {
    	 //maxResources小于等于0  weight小于等于0 ,当求FairShare时，如果队列是非active时
    	  //如果是fix队列，则将其steady fair share 或者 fair share设置为fixedShare
        setResourceValue(fixedShare,
            isSteadyShare
                ? ((FSQueue)sched).getSteadyFairShare()
                : sched.getFairShare(),
            type);
        totalResource = (int) Math.min((long)totalResource + (long)fixedShare,
            Integer.MAX_VALUE);
      }
    }
    return totalResource;
  }

  /**
   * Get the fairshare for the {@link Schedulable} if it is fixed, -1 otherwise.
   *
   * The fairshare is fixed if either the maxShare is 0, weight is 0,
   * or the Schedulable is not active for instantaneous fairshare.
   */
  private static int getFairShareIfFixed(Schedulable sched,
      boolean isSteadyShare, ResourceType type) {

    // Check if maxShare is 0
	  //检查最大资源数是否小于0
    if (getResourceValue(sched.getMaxShare(), type) <= 0) {
      return 0;
    }

    // For instantaneous fairshares, check if queue is active
    if (!isSteadyShare &&
        (sched instanceof FSQueue) && !((FSQueue)sched).isActive()) {
      return 0;
    }

    // Check if weight is 0
    // 如果weight <= 0,则，如果minShare小于等于0 ， 返回0，否则，返回minShare，代表如果weight<=0，那么minShare就是fairShare
    if (sched.getWeights().getWeight(type) <= 0) {
      int minShare = getResourceValue(sched.getMinShare(), type);
      return (minShare <= 0) ? 0 : minShare;
    }

    return -1;//这个队列maxShare大于0 并且（isSteadyShare = true 或者 队列是活跃的 ）并且  weight > 0, 则返回 -1
  }

  private static int getResourceValue(Resource resource, ResourceType type) {
    switch (type) {
    case MEMORY:
      return resource.getMemory();
    case CPU:
      return resource.getVirtualCores();
    default:
      throw new IllegalArgumentException("Invalid resource");
    }
  }
  
  private static void setResourceValue(int val, Resource resource, ResourceType type) {
    switch (type) {
    case MEMORY:
      resource.setMemory(val);
      break;
    case CPU:
      resource.setVirtualCores(val);
      break;
    default:
      throw new IllegalArgumentException("Invalid resource");
    }
  }
}
