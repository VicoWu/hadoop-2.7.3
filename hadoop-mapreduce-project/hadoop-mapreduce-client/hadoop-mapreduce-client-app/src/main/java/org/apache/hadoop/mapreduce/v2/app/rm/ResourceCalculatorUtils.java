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

package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;
import org.apache.hadoop.yarn.util.Records;

import java.util.EnumSet;

public class ResourceCalculatorUtils {
  public static int divideAndCeil(int a, int b) {
    if (b == 0) {
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  /**
   * 
   * @param available ResourceManager返回的container的资源
   * @param required 向ResourceManager请求的资源
   * @param resourceTypes 资源类型
   * @return 1 资源够用 0 资源不够用
   */
  public static int computeAvailableContainers(Resource available,
      Resource required, EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) { //如果资源类型包含cpu资源
      return Math.min(available.getMemory() / required.getMemory(),
        available.getVirtualCores() / required.getVirtualCores());//当且仅当可用的内存大于需要的内存，并且可用的核数大于需要的核数
    }
    return available.getMemory() / required.getMemory(); //可用的内存是否大于需要的内存
  }

  public static int divideAndCeilContainers(Resource required, Resource factor,
      EnumSet<SchedulerResourceTypes> resourceTypes) {
    if (resourceTypes.contains(SchedulerResourceTypes.CPU)) {
      return Math.max(divideAndCeil(required.getMemory(), factor.getMemory()),
        divideAndCeil(required.getVirtualCores(), factor.getVirtualCores()));
    }
    return divideAndCeil(required.getMemory(), factor.getMemory());
  }
}
