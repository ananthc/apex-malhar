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

  void runCommands(List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException;

  <T> T executeMethodCall(String nameOfGlobalMethod, List<Object> argsToGlobalMethod,long timeout, TimeUnit timeUnit,
     T expectedReturnType) throws ApexPythonInterpreterException;

  void executeScript(String scriptName, Map<String,Object> methodParams, long timeout, TimeUnit timeUnit)
      throws ApexPythonInterpreterException;

  <T> T eval(String command, String variableNameToFetch, Map<String,Object> globalMethodsParams,long timeout,
      TimeUnit timeUnit, T expectedReturnType) throws ApexPythonInterpreterException;

  void stopInterpreter() throws ApexPythonInterpreterException;

}
