package org.apache.apex.malhar.python.base;

import java.util.Collection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.jep.JepPythonEngine;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;

@Stateless
public abstract class BasePythonExecutionOperator<T> extends BaseOperator implements
    Operator.ActivationListener<Context.OperatorContext>, Partitioner<BasePythonExecutionOperator>, StatsListener
{
  private static final Logger LOG = LoggerFactory.getLogger(BasePythonExecutionOperator.class);

  private transient long requestIdForThisWindow = 0;

  private transient ApexPythonEngine apexPythonEngine;

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

  }

  @Override
  public void deactivate()
  {

  }

  protected ApexPythonEngine initApexPythonEngineImpl()
  {
    return new JepPythonEngine();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    apexPythonEngine = initApexPythonEngineImpl();
    try {
      apexPythonEngine.preInitInterpreter(getPreInitConfigurations());
      apexPythonEngine.startInterpreter();
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
    return null;
  }

  @Override
  public void partitioned(Map<Integer, Partition<BasePythonExecutionOperator>> partitions)
  {

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

}
