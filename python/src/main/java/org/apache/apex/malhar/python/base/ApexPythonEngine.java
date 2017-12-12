package org.apache.apex.malhar.python.base;

import java.util.Map;

import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

/**
 * An interface that allows implementations to provide a mechanism to return back a type T by running a
 * a python method.
 */
public interface ApexPythonEngine
{
  void preInitInterpreter(Map<String,Object> preInitConfigs) throws ApexPythonInterpreterException;

  void startInterpreter() throws ApexPythonInterpreterException;

  void postStartInterpreter() throws ApexPythonInterpreterException;

  Map<String,PythonRequestResponse<Void>> runCommands(WorkerExecutionMode executionMode, long windowId, long requestId,
      PythonInterpreterRequest<Void> request) throws ApexPythonInterpreterException;

  <T> Map<String,PythonRequestResponse<T>> executeMethodCall(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<T> req) throws ApexPythonInterpreterException;

  Map<String,PythonRequestResponse<Void>>  executeScript(WorkerExecutionMode executionMode,long windowId,long requestId,
      PythonInterpreterRequest<Void> request) throws ApexPythonInterpreterException;

  <T> Map<String,PythonRequestResponse<T>> eval(WorkerExecutionMode executionMode, long windowId, long requestId,
      PythonInterpreterRequest<T> req)  throws ApexPythonInterpreterException;

  long getNumStarvedReturns();

  void setNumStarvedReturns(long numStarvedReturns);

  void stopInterpreter() throws ApexPythonInterpreterException;

}
