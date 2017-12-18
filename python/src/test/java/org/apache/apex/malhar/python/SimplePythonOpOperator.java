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
package org.apache.apex.malhar.python;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.apex.malhar.python.base.ApexPythonEngine;
import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.BasePythonExecutionOperator;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
import org.apache.apex.malhar.python.base.util.NDimensionalArray;
import org.apache.apex.malhar.python.base.util.PythonRequestResponseUtil;

public class SimplePythonOpOperator extends BasePythonExecutionOperator<PythonProcessingPojo>
{
  private Map<String,PythonRequestResponse<NDimensionalArray>> lastKnownResponse;


  @Override
  public PythonRequestResponse processPython(PythonProcessingPojo input, ApexPythonEngine pythonEngineRef)
    throws ApexPythonInterpreterException
  {
    Map<String,Object> evalParams = new HashMap<>();
    evalParams.put("intArrayToAdd",input.getNumpyIntArray());
    evalParams.put("floatArrayToAdd",input.getNumpyFloatArray());
    String evalCommand = "np.add(intMatrix,intArrayToAdd)";
    PythonInterpreterRequest<NDimensionalArray> request = PythonRequestResponseUtil.buildRequestForEvalCommand(
        evalCommand,evalParams,"intMatrix",false, 300,
        TimeUnit.MILLISECONDS, NDimensionalArray.class);
    lastKnownResponse = pythonEngineRef.eval(
        WorkerExecutionMode.ALL_WORKERS,currentWindowId,requestIdForThisWindow,request);
    return lastKnownResponse.get(0);
  }

  @Override
  public void processPostSetUpPythonInstructions(ApexPythonEngine pythonEngineRef) throws ApexPythonInterpreterException
  {
    List<String> commandsToRun = new ArrayList<>();
    commandsToRun.add("import numpy as np");
    commandsToRun.add("intMatrix = np.zeros((2,2),dtype=int)");
    commandsToRun.add("floatMatrix = np.zeros((2,2),dtype=float)");
    pythonEngineRef.runCommands(WorkerExecutionMode.ALL_WORKERS,0L,0L,
        PythonRequestResponseUtil.buildRequestObjectForRunCommands(commandsToRun,200, TimeUnit.MILLISECONDS));
  }

  public Map<String, PythonRequestResponse<NDimensionalArray>> getLastKnownResponse()
  {
    return lastKnownResponse;
  }

  public void setLastKnownResponse(Map<String, PythonRequestResponse<NDimensionalArray>> lastKnownResponse)
  {
    this.lastKnownResponse = lastKnownResponse;
  }
}
