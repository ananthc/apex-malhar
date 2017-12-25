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

import org.apache.apex.malhar.python.base.jep.InterpreterThread;
import org.apache.apex.malhar.python.base.jep.JepPythonEngine;
import org.apache.apex.malhar.python.base.jep.SpinPolicy;
import org.apache.apex.malhar.python.base.partitioner.AbstractPythonExecutionPartitioner;
import org.apache.apex.malhar.python.base.partitioner.PythonExecutionPartitionerType;
import org.apache.apex.malhar.python.base.partitioner.ThreadStarvationBasedPartitioner;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
import org.apache.apex.malhar.python.base.util.NDimensionalArray;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

/***
 * <p>The operator provides a mechanism to execute arbitrary python code by using the {@link JepPythonEngine} as its
 *  default engine.</p>
 *
 * <p>See {@link JepPythonEngine} and {@link InterpreterThread} for more detailed javadocs about the interpreter
 *  itself and the API patterns possible through this engine</p>
 *
 * <p>Note that the JVM option of the library path needs to be set to a value which contains the JEP dynamic library.
 *  For example, -Djava.library.path=/usr/local/lib/python3.5/site-packages/jep assuming that the JEP library is
 *   installed at the above location
 * </p>
 *
 * <p>If using CPython libraries which involve global variables, please use the
 *  {@link PythonInterpreterConfig#PYTHON_SHARED_LIBS} as one of the keys to specify this library as a shared library.
 *  Note that the interpreter configs are passed via {@link ApexPythonEngine#preInitInterpreter(Map)} method.
 *  Overrriding the {@link BasePythonExecutionOperator#getPreInitConfigurations()} and specifying the configs can
 *   help in specifying the shared libraries that are loaded by all the interpreter threads accordingly.</p>
 *
 * <p>The operator comes with the following limitations
 * <ol>
 *   <li>Cannot be used in THREAD LOCAL MODE where the downstream operator is using a different version of the
 *    the python interpreter</li>
 *   <li>In certain use cases the operator cannot be used CONTAINER LOCAL MODE when there are global defs in the
 *    CPython library that is being used and the downstream operator depends on those globals ( even though downstream
 *     is using the same python version of the upstream operator</li>
 *   <li>Only CPython libraries are supported and uses the JNI mechanisms to use the CPython libraries</li>
 *   <li>Complex data types cannot be automatically translated to the Python interpreter space. Current
 *    workaround involves copying the basic types and build the complex type using python code and functionality
 *     provided by the {@link JepPythonEngine} </li>
 *   <li>Numpy arrays need to be specified as {@link NDimensionalArray} and the engine automatically translates them
 *    to a Numpy array. See {@link NDimensionalArray#toNDArray()} for more details</li>
 * </ol>
 * </p>
 *  <p>
 *    Shared libraries enable sharing of global modules across interpreter workers in a work pool.
 *     The following snippet of code illustrated the registering of numpy as a shared module.
 *     <code>
 *       Map<PythonInterpreterConfig,Object> preInitConfigs = new HashMap<>();
 *       Set<String> sharedLibsList = new HashSet<>();
 *       sharedLibsList.add("numpy");
 *       preInitConfigs.put(PythonInterpreterConfig.PYTHON_SHARED_LIBS, sharedLibsList);
 *     </code>
 *     Passing the above config in overriding {@link BasePythonExecutionOperator#getPreInitConfigurations()} will help
 *      in using modules like numpy which have global shared variables.
 *  </p>
 *
 *  <p>For very low time SLA requirements, it is encouraged to set the spin policy to be busy wait using the
 *   following configuration snippet for the JEP engine
 *   <code>
 *       Map<PythonInterpreterConfig,Object> preInitConfigs = new HashMap<>();
 *       preInitConfigs.put(PythonInterpreterConfig.SPIN_POLICY, ""+ SpinPolicy.BUSY_SPIN);
 *   </code>
 *   This mapping can be set by overriding {@link BasePythonExecutionOperator#getPreInitConfigurations()}. For more
 *    details refer {@link SpinPolicy}.
 *  </p>
 * @param <T> Represents the incoming tuple.
 */
public class BasePythonExecutionOperator<T> extends BaseOperator implements
    Operator.ActivationListener<Context.OperatorContext>, Partitioner<BasePythonExecutionOperator>,
    Operator.CheckpointNotificationListener, StatsListener, Operator.IdleTimeHandler
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BasePythonExecutionOperator.class);

  /*  a rolling counter for all requests in the current window */
  protected transient long requestIdForThisWindow = 0;

  protected transient long currentWindowId = 0;

  private long numberOfRequestsProcessedPerCheckpoint = 0;

  /*** Configuration used to decide if a new operator needs to be spawned while dynamic partitioning planning is taking
   place. This represents the threshold for percent number of times the requests were starved when no threads were
    available to process a request. See {@link ThreadStarvationBasedPartitioner}
   */
  private float starvationPercentBeforeSpawningNewInstance = 30;

  private transient ApexPythonEngine apexPythonEngine;

  /*** number of workers in the worker pool */
  private int workerThreadPoolSize = 3;

  /*** Time for which the python engine will sleep to allow for the interpreter will sleep before worker can be used
   *  for executing work requests.
   */
  private long sleepTimeDuringInterpreterBoot = 2000L;

  private PythonExecutionPartitionerType partitionerType = PythonExecutionPartitionerType.THREAD_STARVATION_BASED;

  private transient AbstractPythonExecutionPartitioner partitioner;

  /*** port into which the stragglers will be emitted */
  public final transient DefaultOutputPort<PythonRequestResponse> stragglersPort =
      new com.datatorrent.api.DefaultOutputPort<>();

  /*** Port into which the normal execution path will push the results into */
  public final transient DefaultOutputPort<PythonRequestResponse> outputPort =
      new com.datatorrent.api.DefaultOutputPort<>();

  /*** Port into which all error tuples will be emitted into */
  public final transient DefaultOutputPort<T> errorPort = new com.datatorrent.api.DefaultOutputPort<>();

  private Object objectForLocking = new Object();

  /*** A counter that is used to track how many times a request could not be serviced within a given window. Used
   by the {@link ThreadStarvationBasedPartitioner} to spawn a new instance of the operator based on a configuration
   threshold
   */
  @AutoMetric
  private long numStarvedReturnsPerCheckpoint = 0;

  @AutoMetric
  private long numNullResponsesPerWindow = 0;

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
          numNullResponsesPerWindow += 1;
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
      Map<PythonInterpreterConfig,Object> preInitConfigurations = getPreInitConfigurations();
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
    numNullResponsesPerWindow = 0;
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
    numStarvedReturnsPerCheckpoint = getApexPythonEngine().getNumStarvedReturns();
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

  /***
   * See constants defined in {@link PythonInterpreterConfig} for a list of keys available
   * @return
   */
  public Map<PythonInterpreterConfig,Object> getPreInitConfigurations()
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
    return numStarvedReturnsPerCheckpoint;
  }

  public void setNumStarvedReturns(long numStarvedReturns)
  {
    this.numStarvedReturnsPerCheckpoint = numStarvedReturns;
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
