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
  /**
   * 给出一系列的Schedulable,在预先规定了各个Schedulable的minShare和maxShare的情况下，
   * 我们的目的，就是根据minShare和maxShare,综合整个集群的全体资源，为各个Schedulable进行最公平
   * 的分配。
   * 为了理解这种分配策略，我们必须理解，在给定minShare和maxShare的前提下，加权的公平分配代表什么：
   * 如果没有minShare和maxShare，那么所有Schedulable权重相同，它们通过*均分*集群资源得到属于自
   * 己的fair share。但是当每个队列有了不同的minShare和maxShare,那么权重就各不相同了，最终可能
   * 有些队列分得的fairShare小于自己的minShare，有些队列分得的fairShare大于自己的maxShare。
   * 为了处理这种可能性，我们假定如果存在这样一个R值，并满足一下条件，那么就可以认为已经做到了公平分配：
   * 对于一个Schedulable S，如果它的minShare > R * S.weight，那么这个Schedulable被分配的fair 
   * share 是它的minShare,并且，如果它的maxShare < R * S.weight ,那么这个Schedulable被分配
   * 的fairShare是它的maxShare。其它的Schedulable被分配的资源是R * S.weight。总体资源之和是
   * 总资源。
   * 我们把R叫做资源权重比值，因为它把一个Schedulable的权重转换成为了Schedulable实际分配到的资源。
   * 我们计算一个Schedulable，就是通过找到一个合适的资源权重比（R）来进行的。我们通过二分查找来得到R。
   * 给出一个R的启动值，我们计算总体使用资源，如果这个总体使用资源小于总资源，那么R太小了，需要增大；相反，
   * R太大了， 需要减小。
   * 开始二分查找前，我们设定R的下限值和上限值， 下限值为0，意味着所有的Schedulable都只能分配到自己的
   * minShare的资源，上限值则被设置为足够大以至于所有的Scheduler得到的资源已经大于集群总资源（通过将
   * R从1开始不断翻倍直到所有Schedulable分配到的资源大于集群总资源）。从上述规则我们知道，如果R=0,
   * 意味着所有的Schedulable都只被分配自己的minShare
   * (Yarn规定，在 fair-scheduler.xml中配置的所有Schedulable的minShare之和必须小于Yarn的所有
   * NodeManager的资源之和，否则fair-scheduler不生效)。
   * @param allSchedulables
   * @param totalResources
   * @param type
   * @param isSteadyShare
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
    //找到一个我们准备用在二分查找的R的上限值，我们将R初始化为1，然后每次翻倍，直到所有的Schedulable
    //已经用完了所有的资源或者所有Schedulables已经达到了自己的maxShare
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

    //集群总资源，减去 已经计算完毕的fix队列的资源，得到剩下的non-fix的资源总量，这一部分资源，是可分配的资源
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
    //为了防止无限迭代下去，设置COMPUTE_FAIR_SHARES_ITERATIONS限制迭代
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
  /**
   * 计算fixed-schedulable的固定资源,fix-schedulable的资源不具有动态性，
   * 因此可以直接进行计算。那么，什么叫做fixed-schedulable呢：
   * 可以参考getFairShareIfFixed()方法的注解。
   * @param schedulables
   * @param nonFixedSchedulables
   * @param isSteadyShare
   * @param type
   * @return
   */
  private static int handleFixedFairShares(
      Collection<? extends Schedulable> schedulables,
      Collection<Schedulable> nonFixedSchedulables,
      boolean isSteadyShare, ResourceType type) {
    int totalResource = 0;//所有队列的总资源求和

    for (Schedulable sched : schedulables) {
      //如果是一个fixed队列，则fixedShare为该队列的minShare或者0
      int fixedShare = getFairShareIfFixed(sched, isSteadyShare, type);
      if (fixedShare < 0) {
    	//不是fixed，即maxShare 和weight不是0， 并且这个Schedulable是一个active的
    	//则将这个Schedulable保存在nonFixedSchedulables中返回
        nonFixedSchedulables.add(sched);
      } else {
    	//如果是fix队列，那么分两种情况：
    	// 如果isSteadyShare=true,即我们计算的是steady fairshares,则将其steady fair share设置为fixedShare
    	// 如果isSteadyShare=false,即我们计算的instaneous fair share,则将这个Schedulable的fairShare(
    	//即instaneous fair share)设置为fixedShare
        setResourceValue(fixedShare,
            isSteadyShare
                ? ((FSQueue)sched).getSteadyFairShare()
                : sched.getFairShare(),
            type);
        totalResource = (int) Math.min((long)totalResource + (long)fixedShare,
            Integer.MAX_VALUE);
      }
    }
    //返回我们计算得到的所有的 fix-schedulable的资源之和
    return totalResource;
  }

  /**
   * Get the fairshare for the {@link Schedulable} if it is fixed, -1 otherwise.
   *
   * The fairshare is fixed if either the maxShare is 0, weight is 0,
   * or the Schedulable is not active for instantaneous fairshare.
   * 如果这个Schedulable是一个fixed schedulable，则返回这个fixed-sheculable的资源份额。
   * fixed-schedulable的定义是：
   * 这个Schedulbale的maxShare <=0
   * 或者
   * 当求instantaneous fair share, 并且这个队列是非活跃队列
   * 或者
   * weight <= 0
   */
  private static int getFairShareIfFixed(Schedulable sched,
      boolean isSteadyShare, ResourceType type) {

    // Check if maxShare is 0
	  //检查最大资源数是否小于0
    if (getResourceValue(sched.getMaxShare(), type) <= 0) {
      return 0;
    }

    // For instantaneous fairshares, check if queue is active
    //isSteadyShare为true,代表我们计算的是steady share,此时不区分这个队列
    //是不是active的， 但是如果isSteadyShare 是false，代表我们目前计算的
    //是instantaneous fairshares，此时，只要队列不是活跃的，就认为是
    //fix-schedulable
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
    //这个队列maxShare大于0 并且（isSteadyShare = true 或者 队列是活跃的 ）并且  weight > 0, 则返回 -1,代表这不是一个fixed-schedulable
    return -1;
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
