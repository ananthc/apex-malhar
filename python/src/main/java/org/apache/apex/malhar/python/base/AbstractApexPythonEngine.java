package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractApexPythonEngine implements ApexPythonEngine
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractApexPythonEngine.class);

  @Override
  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {

  }

  @Override
  public void startInterpreter() throws ApexPythonInterpreterException
  {

  }

  @Override
  public Map<String, Boolean> runCommands(WorkerExecutionMode executionMode, long windowId, long requestId, List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public <T> T executeMethodCall(WorkerExecutionMode executionMode, long windowId, long requestId, String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit, Class<T> expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public void executeScript(WorkerExecutionMode executionMode, long windowId, long requestId, String scriptName, Map<String, Object> methodParams, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException
  {

  }

  @Override
  public <T> T eval(WorkerExecutionMode executionMode, long windowId, long requestId, String command, String variableNameToFetch, Map<String, Object> globalMethodsParams, long timeout, TimeUnit timeUnit, boolean deleteExtractedVariable, Class<T> expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public void stopInterpreter() throws ApexPythonInterpreterException
  {

  }
}
