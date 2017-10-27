package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;

/**
 * An interface that allows implementations to provide a mechanism to return back a type T by running a
 * a python method.
 */
public interface ApexPythonEngine<T>
{
  void preInitInterpreter(Map<String,Object> preInitConfigs) throws ApexPythonInterpreterException;

  void startInterpreter() throws ApexPythonInterpreterException;

  void runCommands(List<String> commands) throws ApexPythonInterpreterException;

  T executeMethodCall(String nameOfGlobalMethod, List<Object> argsToGlobalMethod) throws ApexPythonInterpreterException;

  void executeScript(String scriptName, Map<String,Object> methodParams) throws ApexPythonInterpreterException;

  T eval(String command, String variableNameToFetch, Map<String,Object> globalMethodsParams)
      throws ApexPythonInterpreterException;

  void stopInterpreter() throws ApexPythonInterpreterException;

}
