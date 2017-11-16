package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An interface that allows implementations to provide a mechanism to return back a type T by running a
 * a python method.
 */
public interface ApexPythonEngine
{
  void preInitInterpreter(Map<String,Object> preInitConfigs) throws ApexPythonInterpreterException;

  void startInterpreter() throws ApexPythonInterpreterException;

  Map<String,Boolean>  runCommands(WorkerExecutionMode executionMode, long windowId, long requestId,
      List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException;

  <T> T executeMethodCall(WorkerExecutionMode executionMode, long windowId, long requestId,
      String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit,
      Class<T> expectedReturnType) throws ApexPythonInterpreterException, TimeoutException;

  void executeScript(WorkerExecutionMode executionMode,long windowId, long requestId,
      String scriptName, Map<String,Object> methodParams, long timeout, TimeUnit timeUnit)
    throws ApexPythonInterpreterException, TimeoutException;

  <T> T eval(WorkerExecutionMode executionMode, long windowId, long requestId, String command,
      String variableNameToFetch, Map<String,Object> globalMethodsParams,long timeout, TimeUnit timeUnit,
      boolean deleteExtractedVariable, Class<T> expectedReturnType)
    throws ApexPythonInterpreterException,TimeoutException;

  void stopInterpreter() throws ApexPythonInterpreterException;

}
