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
package org.apache.apex.malhar.python.base.requestresponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PythonRequestResponseUtil
{
  public static PythonInterpreterRequest<Void> buildRequestObjectForRunCommands(List<String> commands, long timeOut,
      TimeUnit timeUnit)
  {
    GenericCommandsRequestPayload genericCommandsRequestPayload = new GenericCommandsRequestPayload();
    genericCommandsRequestPayload.setGenericCommands(commands);
    PythonInterpreterRequest<Void> request = new PythonInterpreterRequest<>(Void.class);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setGenericCommandsRequestPayload(genericCommandsRequestPayload);
    return request;
  }

  public static <T> PythonInterpreterRequest<T> buildRequestForEvalCommand(
      String evalCommand, Map<String,Object> evalParams, String varNameToExtract,
      boolean deleteVarAfterExtract, long timeOut, TimeUnit timeUnit, Class<T> clazz)
  {
    PythonInterpreterRequest<T> request = new PythonInterpreterRequest<>(clazz);
    EvalCommandRequestPayload evalCommandRequestPayload = new EvalCommandRequestPayload();
    evalCommandRequestPayload.setEvalCommand(evalCommand);
    evalCommandRequestPayload.setVariableNameToExtractInEvalCall(varNameToExtract);
    evalCommandRequestPayload.setParamsForEvalCommand(evalParams);
    evalCommandRequestPayload.setDeleteVariableAfterEvalCall(deleteVarAfterExtract);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setEvalCommandRequestPayload(evalCommandRequestPayload);
    return request;
  }


  public static <T> PythonInterpreterRequest<T> buildRequestForMethodCallCommand(
      String methodName, List<Object> methodParams, long timeOut, TimeUnit timeUnit, Class<T> clazz)
  {
    PythonInterpreterRequest<T> request = new PythonInterpreterRequest<>(clazz);
    MethodCallRequestPayload methodCallRequestPayload = new MethodCallRequestPayload();
    methodCallRequestPayload.setNameOfMethod(methodName);
    methodCallRequestPayload.setArgs(methodParams);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setMethodCallRequest(methodCallRequestPayload);
    return request;
  }


  public static <T> PythonInterpreterRequest<T> buildRequestForScriptdCallCommand(
      String scriptPath, long timeOut, TimeUnit timeUnit, Class<T> clazz)
  {
    PythonInterpreterRequest<T> request = new PythonInterpreterRequest<>(clazz);
    ScriptExecutionRequestPayload scriptExecutionRequestPayload = new ScriptExecutionRequestPayload();
    scriptExecutionRequestPayload.setScriptName(scriptPath);
    request.setTimeUnit(timeUnit);
    request.setTimeout(timeOut);
    request.setScriptExecutionRequestPayload(scriptExecutionRequestPayload);
    return request;
  }

}
