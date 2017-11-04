package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JepPythonSingleThreadedEngine extends AbstractApexPythonEngine
{
  private transient JepPythonExecutor jepPythonExecutor;

  private static class JepPythonCommandsExecutor<T> implements Callable<T>
  {

    public JepPythonCommandsExecutor(JepPythonExecutor jepPythonExecutor)
    {
    }

    @Override
    public T call() throws Exception
    {
      return null;
    }
  }

  ExecutorService executorService = Executors.newFixedThreadPool(1);

  public JepPythonSingleThreadedEngine()
  {
    jepPythonExecutor = new JepPythonExecutor();
  }

  @Override
  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    jepPythonExecutor.preInitInterpreter(preInitConfigs);
  }

  @Override
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    jepPythonExecutor.startInterpreter();
  }

  @Override
  public Map<String, Boolean> runCommands(List<String> commands, long timeout, TimeUnit timeUnit)
      throws ApexPythonInterpreterException, TimeoutException
  {
    jepPythonExecutor.runCommands(commands);
    return null;
  }

  @Override
  public <T> T executeMethodCall(String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout,
      TimeUnit timeUnit, T expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public void executeScript(String scriptName, Map<String, Object> methodParams, long timeout, TimeUnit timeUnit)
      throws ApexPythonInterpreterException, TimeoutException
  {

  }

  @Override
  public <T> T eval(String command, String variableNameToFetch, Map<String, Object> globalMethodsParams, long timeout,
      TimeUnit timeUnit, T expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public void stopInterpreter() throws ApexPythonInterpreterException
  {

  }
}
