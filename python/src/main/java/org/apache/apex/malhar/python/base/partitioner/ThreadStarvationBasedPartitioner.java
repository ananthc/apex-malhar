/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.base.partitioner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.BasePythonExecutionOperator;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

public class ThreadStarvationBasedPartitioner extends AbstractPythonExecutionPartitioner
{
  private static final Logger LOG = LoggerFactory.getLogger(ThreadStarvationBasedPartitioner.class);

  private float threadStarvationThresholdRatio;

  public ThreadStarvationBasedPartitioner(BasePythonExecutionOperator prototypePythonOperator)
  {
    super(prototypePythonOperator);
  }

  @Override
  protected List<Partition<BasePythonExecutionOperator>> calculateNumPartitions(
      Collection<Partition<BasePythonExecutionOperator>> partitions, PartitioningContext context)
  {
    List<Partition<BasePythonExecutionOperator>> returnList = new ArrayList<>();
    if (partitions != null) {
      returnList.addAll(partitions);
      for (Partition<BasePythonExecutionOperator> aCurrentPartition : partitions) {
        BasePythonExecutionOperator anOperator = aCurrentPartition.getPartitionedInstance();
        long starvedCount = anOperator.getNumStarvedReturns();
        long requestsForCheckpointWindow = anOperator.getNumberOfRequestsProcessedPerCheckpoint();
        if ( requestsForCheckpointWindow != 0) { // when the operator is starting for the first time
          float starvationPercent = 100 - ( ((requestsForCheckpointWindow - starvedCount ) /
              requestsForCheckpointWindow) * 100) ;
          if (starvationPercent > anOperator.getStarvationPercentBeforeSpawningNewInstance()) {
            Partition<BasePythonExecutionOperator> newInstance = clonePartition();
            List<PythonRequestResponse> commandHistory = new ArrayList<>();
            commandHistory.addAll(anOperator.getAccumulatedCommandHistory());
            newInstance.getPartitionedInstance().setAccumulatedCommandHistory(commandHistory);
            returnList.add(newInstance);
          }
        }
      }
    }
    return returnList;
  }

  public float getThreadStarvationThresholdRatio()
  {
    return threadStarvationThresholdRatio;
  }

  public void setThreadStarvationThresholdRatio(float threadStarvationThresholdRatio)
  {
    this.threadStarvationThresholdRatio = threadStarvationThresholdRatio;
  }
}
