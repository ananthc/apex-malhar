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
package org.apache.apex.malhar.python.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.jep.JepPythonEngine;
import org.apache.apex.malhar.python.base.partitioner.AbstractPythonExecutionPartitioner;
import org.apache.apex.malhar.python.base.partitioner.PythonExecutionPartitionerType;
import org.apache.apex.malhar.python.base.partitioner.ThreadStarvationBasedPartitioner;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public abstract class BasePythonExecutionOperator<T> extends BaseOperator implements
    Operator.ActivationListener<Context.OperatorContext>, Partitioner<BasePythonExecutionOperator>,
    Operator.CheckpointNotificationListener, StatsListener, Operator.IdleTimeHandler
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BasePythonExecutionOperator.class);

  protected transient long requestIdForThisWindow = 0;

  protected transient long currentWindowId = 0;

  private long numberOfRequestsProcessedPerCheckpoint = 0;

  private float starvationPercentBeforeSpawningNewInstance = 30;

  private transient ApexPythonEngine apexPythonEngine;

  private int workerThreadPoolSize = 3;

  private long sleepTimeDuringInterpreterBoot = 2000L;

  private PythonExecutionPartitionerType partitionerType = PythonExecutionPartitionerType.THREAD_STARVATION_BASED;

  private transient AbstractPythonExecutionPartitioner partitioner;

  public final transient DefaultOutputPort<PythonRequestResponse> stragglersPort =
      new com.datatorrent.api.DefaultOutputPort<>();

  public final transient DefaultOutputPort<PythonRequestResponse> outputPort =
      new com.datatorrent.api.DefaultOutputPort<>();

  public final transient DefaultOutputPort<T> errorPort = new com.datatorrent.api.DefaultOutputPort<>();

  private Object objectForLocking = new Object();

  @AutoMetric
  private long numStarvedReturns = 0;

  @AutoMetric
  private long numNullResponses = 0;

  private List<PythonRequestResponse> accumulatedCommandHistory = new ArrayList<>();

  @InputPortFieldAnnotation
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      numberOfRequestsProcessedPerCheckpoint += 1;
      emitStragglers();
      try {
        PythonRequestResponse result = processPython(tuple,getApexPythonEngine());
        if ( result != null) {
          outputPort.emit(result);
        } else {
          numNullResponses += 1;
        }
      } catch (ApexPythonInterpreterException e) {
        errorPort.emit(tuple);
        LOG.error("Error while processing tuple", e);
      }
    }
  };

  @Override
  public void activate(Context.OperatorContext context)
  {
    getApexPythonEngine().setNumStarvedReturns(0L);
  }

  @Override
  public void deactivate()
  {

  }

  private void emitStragglers()
  {
    List<PythonRequestResponse> stragglerResponse = new ArrayList<>();
    getApexPythonEngine().getDelayedResponseQueue().drainTo(stragglerResponse);
    for (PythonRequestResponse aReqResponse : stragglerResponse) {
      stragglersPort.emit(aReqResponse);
    }
  }
  protected ApexPythonEngine initApexPythonEngineImpl(Context.OperatorContext context)
  {
    JepPythonEngine jepPythonEngine = new JepPythonEngine("" + context.getId(),workerThreadPoolSize);
    jepPythonEngine.setSleepTimeAfterInterpreterStart(sleepTimeDuringInterpreterBoot);
    return jepPythonEngine;
  }

  private void initPartitioner()
  {
    if (partitioner == null) {
      synchronized (objectForLocking) {
        if (partitioner == null) {
          switch (partitionerType) {
            default:
            case THREAD_STARVATION_BASED:
              partitioner = new ThreadStarvationBasedPartitioner(this);
              break;
          }
        }
      }
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    apexPythonEngine = initApexPythonEngineImpl(context);
    try {
      Map<String,Object> preInitConfigurations = getPreInitConfigurations();
      apexPythonEngine.preInitInterpreter(preInitConfigurations);
      apexPythonEngine.startInterpreter();
      apexPythonEngine.postStartInterpreter();
    } catch (ApexPythonInterpreterException e) {
      throw new RuntimeException(e);
    }
    initPartitioner();
    try {
      processPostSetUpPythonInstructions(apexPythonEngine);
    } catch (ApexPythonInterpreterException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    super.teardown();
    try {
      apexPythonEngine.stopInterpreter();
    } catch (ApexPythonInterpreterException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    requestIdForThisWindow = 0;
    currentWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    numNullResponses = 0;
  }

  @Override
  public Collection<Partition<BasePythonExecutionOperator>> definePartitions(
      Collection<Partition<BasePythonExecutionOperator>> partitions, PartitioningContext context)
  {
    initPartitioner();
    return partitioner.definePartitions(partitions,context);
  }

  @Override
  public void partitioned(Map<Integer, Partition<BasePythonExecutionOperator>> partitions)
  {
    initPartitioner();
    partitioner.partitioned(partitions);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    accumulatedCommandHistory.clear();
    accumulatedCommandHistory.addAll(getApexPythonEngine().getCommandHistory());
    numStarvedReturns = getApexPythonEngine().getNumStarvedReturns();
  }

  @Override
  public void checkpointed(long windowId)
  {
    getApexPythonEngine().setNumStarvedReturns(0L);
    numberOfRequestsProcessedPerCheckpoint = 0;
  }

  @Override
  public void committed(long windowId)
  {
  }

  public void processPostSetUpPythonInstructions(ApexPythonEngine pythonEngine) throws ApexPythonInterpreterException
  {
  }

  @Override
  public void handleIdleTime()
  {
    emitStragglers();
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    Response response = new Response();
    if ( apexPythonEngine == null) {
      response.repartitionRequired = true;
    } else {
      long starvedCount = apexPythonEngine.getNumStarvedReturns();
      long requestsForCheckpointWindow = getNumberOfRequestsProcessedPerCheckpoint();
      float starvationPercent = ((requestsForCheckpointWindow - starvedCount) / requestsForCheckpointWindow) * 100;
      if (starvationPercent > getStarvationPercentBeforeSpawningNewInstance()) {
        response.repartitionRequired = true;
      }
    }
    return response;
  }

  public ApexPythonEngine getApexPythonEngine()
  {
    return apexPythonEngine;
  }

  public void setApexPythonEngine(ApexPythonEngine apexPythonEngine)
  {
    this.apexPythonEngine = apexPythonEngine;
  }

  public PythonRequestResponse processPython(T input, ApexPythonEngine pythonEngineRef)
    throws ApexPythonInterpreterException
  {
    return null;
  }

  public Map<String,Object> getPreInitConfigurations()
  {
    return new HashMap<>();
  }

  public long getSleepTimeDuringInterpreterBoot()
  {
    return sleepTimeDuringInterpreterBoot;
  }

  public void setSleepTimeDuringInterpreterBoot(long sleepTimeDuringInterpreterBoot)
  {
    this.sleepTimeDuringInterpreterBoot = sleepTimeDuringInterpreterBoot;
  }

  public int getWorkerThreadPoolSize()
  {
    return workerThreadPoolSize;
  }

  public void setWorkerThreadPoolSize(int workerThreadPoolSize)
  {
    this.workerThreadPoolSize = workerThreadPoolSize;
  }

  public PythonExecutionPartitionerType getPartitionerType()
  {
    return partitionerType;
  }

  public void setPartitionerType(PythonExecutionPartitionerType partitionerType)
  {
    this.partitionerType = partitionerType;
  }

  public long getNumberOfRequestsProcessedPerCheckpoint()
  {
    return numberOfRequestsProcessedPerCheckpoint;
  }

  public void setNumberOfRequestsProcessedPerCheckpoint(long numberOfRequestsProcessedPerCheckpoint)
  {
    this.numberOfRequestsProcessedPerCheckpoint = numberOfRequestsProcessedPerCheckpoint;
  }

  public AbstractPythonExecutionPartitioner getPartitioner()
  {
    return partitioner;
  }

  public void initPartitioner(AbstractPythonExecutionPartitioner partitioner)
  {
    this.partitioner = partitioner;
  }

  public float getStarvationPercentBeforeSpawningNewInstance()
  {
    return starvationPercentBeforeSpawningNewInstance;
  }

  public void setStarvationPercentBeforeSpawningNewInstance(float starvationPercentBeforeSpawningNewInstance)
  {
    this.starvationPercentBeforeSpawningNewInstance = starvationPercentBeforeSpawningNewInstance;
  }

  public long getNumStarvedReturns()
  {
    return numStarvedReturns;
  }

  public void setNumStarvedReturns(long numStarvedReturns)
  {
    this.numStarvedReturns = numStarvedReturns;
  }

  public List<PythonRequestResponse> getAccumulatedCommandHistory()
  {
    return accumulatedCommandHistory;
  }

  public void setAccumulatedCommandHistory(List<PythonRequestResponse> accumulatedCommandHistory)
  {
    this.accumulatedCommandHistory = accumulatedCommandHistory;
  }
}
