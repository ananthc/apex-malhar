package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JepPythonEngine extends AbstractApexPythonEngine
{
  private transient JepPythonExecutor jepPythonExecutor;

  public JepPythonEngine()
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
