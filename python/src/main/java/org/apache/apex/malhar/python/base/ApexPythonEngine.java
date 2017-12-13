/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

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

  BlockingQueue<PythonRequestResponse> getDelayedResponseQueue();

  void setDelayedResponseQueue(BlockingQueue<PythonRequestResponse> delayedResponseQueue);

  long getNumStarvedReturns();

  void setNumStarvedReturns(long numStarvedReturns);

  List<PythonRequestResponse> getCommandHistory();

  void setCommandHistory(List<PythonRequestResponse> commandHistory);

  void stopInterpreter() throws ApexPythonInterpreterException;

}
