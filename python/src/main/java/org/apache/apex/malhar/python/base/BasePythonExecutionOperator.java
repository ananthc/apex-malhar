package org.apache.apex.malhar.python.base;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.jep.JepPythonEngine;
import org.apache.apex.malhar.python.base.partitioner.AbstractPythonExecutionPartitioner;
import org.apache.apex.malhar.python.base.partitioner.PythonExecutionPartitionerType;
import org.apache.apex.malhar.python.base.partitioner.ThreadStarvationBasedPartitioner;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public abstract class BasePythonExecutionOperator<T> extends BaseOperator implements
    Operator.ActivationListener<Context.OperatorContext>, Partitioner<BasePythonExecutionOperator>, StatsListener
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BasePythonExecutionOperator.class);

  private transient long requestIdForThisWindow = 0;

  private transient ApexPythonEngine apexPythonEngine;

  private int workerThreadPoolSize = 3;

  private long sleepTimeDuringInterpreterBoot = 2000L;

  private PythonExecutionPartitionerType partitionerType = PythonExecutionPartitionerType.THREAD_STARVATION_BASED;

  private AbstractPythonExecutionPartitioner partitioner;

  @InputPortFieldAnnotation
  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      try {
        processPython(tuple,getApexPythonEngine());
      } catch (ApexPythonInterpreterException e) {
        throw new RuntimeException(e);
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

  protected ApexPythonEngine initApexPythonEngineImpl(Context.OperatorContext context)
  {
    JepPythonEngine jepPythonEngine = new JepPythonEngine("" + context.getId(),workerThreadPoolSize);
    jepPythonEngine.setSleepTimeAfterInterpreterStart(sleepTimeDuringInterpreterBoot);
    return jepPythonEngine;
  }

  private void setPartitioner()
  {
    switch (partitionerType) {
      default:
      case THREAD_STARVATION_BASED:
        partitioner = new ThreadStarvationBasedPartitioner();
        break;
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    apexPythonEngine = initApexPythonEngineImpl(context);
    try {
      apexPythonEngine.preInitInterpreter(getPreInitConfigurations());
      apexPythonEngine.startInterpreter();
      apexPythonEngine.postStartInterpreter();
    } catch (ApexPythonInterpreterException e) {
      throw new RuntimeException(e);
    }
    setPartitioner();
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
    getApexPythonEngine().setNumStarvedReturns(0L);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
  }

  @Override
  public Collection<Partition<BasePythonExecutionOperator>> definePartitions(
      Collection<Partition<BasePythonExecutionOperator>> partitions, PartitioningContext context)
  {
    return partitioner.definePartitions(partitions,context);
  }

  @Override
  public void partitioned(Map<Integer, Partition<BasePythonExecutionOperator>> partitions)
  {
    partitioner.partitioned(partitions);
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    return null;
  }

  public ApexPythonEngine getApexPythonEngine()
  {
    return apexPythonEngine;
  }

  public void setApexPythonEngine(ApexPythonEngine apexPythonEngine)
  {
    this.apexPythonEngine = apexPythonEngine;
  }

  public abstract void processPython(T input, ApexPythonEngine pythonEngineRef) throws ApexPythonInterpreterException;

  public abstract Map<String,Object> getPreInitConfigurations();

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
}
