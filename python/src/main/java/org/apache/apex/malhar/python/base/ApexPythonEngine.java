package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An interface that allows implementations to provide a mechanism to return back a type T by running a
 * a python method.
 */
public interface ApexPythonEngine
{
  void preInitInterpreter(Map<String,Object> preInitConfigs) throws ApexPythonInterpreterException;

  void startInterpreter() throws ApexPythonInterpreterException;

  void postStartInterpreter() throws ApexPythonInterpreterException;

  Map<String,PythonRequestResponse> runCommands(WorkerExecutionMode executionMode, long windowId, long requestId,
      List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException;

  <T> Map<String,PythonRequestResponse<T>> executeMethodCall(WorkerExecutionMode executionMode,long windowId,
      long requestId, String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit,
      Class<T> expectedReturnType) throws ApexPythonInterpreterException;

  Map<String,PythonRequestResponse>  executeScript(WorkerExecutionMode executionMode,long windowId, long requestId,
      String scriptName, long timeout, TimeUnit timeUnit)
    throws ApexPythonInterpreterException;

  <T> Map<String,PythonRequestResponse<T>> eval(WorkerExecutionMode executionMode, long windowId, long requestId,
      String command, String variableNameToFetch, Map<String,Object> globalMethodsParams,long timeout,TimeUnit timeUnit,
      boolean deleteExtractedVariable, Class<T> expectedReturnType)
    throws ApexPythonInterpreterException;

  void stopInterpreter() throws ApexPythonInterpreterException;

}
