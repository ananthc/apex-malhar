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

import org.apache.apex.malhar.python.base.ApexPythonEngine;
import org.apache.apex.malhar.python.base.BasePythonExecutionOperator;


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
      for (Partition<BasePythonExecutionOperator> aCurrentPartition : partitions) {
        BasePythonExecutionOperator anOperator = aCurrentPartition.getPartitionedInstance();
        ApexPythonEngine anEngine = anOperator.getApexPythonEngine();
        if (anEngine != null) {
          long starvedCount = anEngine.getNumStarvedReturns();
          long requestsForCheckpointWindow = anOperator.getNumberOfRequestsProcessedPerCheckpoint();
          float starvationPercent = ((requestsForCheckpointWindow - starvedCount ) / requestsForCheckpointWindow) * 100;
          if (starvationPercent > anOperator.getStarvationPercentBeforeSpawningNewInstance()) {
            Partition<BasePythonExecutionOperator> newInstance = clonePartitionAndAssignScanMeta();
            newInstance.getPartitionedInstance().getApexPythonEngine().setCommandHistory(anEngine.getCommandHistory());
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
