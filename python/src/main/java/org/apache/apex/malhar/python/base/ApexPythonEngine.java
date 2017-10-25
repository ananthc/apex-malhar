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

  void postInitInterpreter(List<String> commands) throws ApexPythonInterpreterException;

  T executeCall(Map<String,Object> methodParams) throws ApexPythonInterpreterException;

  T executeScript(Map<String,Object> methodParams) throws ApexPythonInterpreterException;

  void stopInterpreter() throws ApexPythonInterpreterException;

}
