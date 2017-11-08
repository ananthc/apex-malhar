package org.apache.apex.malhar.python.base.jep;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.AbstractApexPythonEngine;
import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

public class JepPythonExecutionEngine extends AbstractApexPythonEngine
{
  private static final Logger LOG = LoggerFactory.getLogger(JepPythonExecutionEngine.class);

  private transient JepPythonCommandExecutor jepPythonCommandExecutor;

  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 16; // Represents the number of workers and response queue sizes

  private transient BlockingQueue<PythonRequestResponse> requestQueue =
    new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private transient BlockingQueue<PythonRequestResponse> responseQueue =
    new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);


  public JepPythonExecutionEngine()
  {
    jepPythonCommandExecutor = new JepPythonCommandExecutor(requestQueue,responseQueue);
  }

  @Override
  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    jepPythonCommandExecutor.preInitInterpreter(preInitConfigs);
  }

  @Override
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    jepPythonCommandExecutor.startInterpreter();
  }

  @Override
  public Map<String, Boolean> runCommands(WorkerExecutionMode executionMode,List<String> commands, long timeout,
      TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException
  {
    jepPythonCommandExecutor.runCommands(commands);
    return null;
  }

  @Override
  public <T> T executeMethodCall(WorkerExecutionMode executionMode,String nameOfGlobalMethod,
      List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit, T expectedReturnType)
      throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public void executeScript(WorkerExecutionMode executionMode, String scriptName, Map<String, Object> methodParams,
      long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException
  {

  }

  @Override
  public <T> T eval(WorkerExecutionMode executionMode, String command, String variableNameToFetch,
      Map<String, Object> globalMethodsParams, long timeout, TimeUnit timeUnit, boolean deleteExtractedVariable,
      T expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public void stopInterpreter() throws ApexPythonInterpreterException
  {

  }
}
