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
package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.requestresponse.EvalCommandRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.MethodCallRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterResponse;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
import org.apache.apex.malhar.python.base.requestresponse.ScriptExecutionRequestPayload;
import org.apache.apex.malhar.python.base.util.NDimensionalArray;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;
import jep.NDArray;

/**
 *
 */

public class InterpreterThread implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterThread.class);

  public static final String JEP_LIBRARY_NAME = "jep";
  public static final String PYTHON_DEL_COMMAND = "del ";

  public static final String PYTHON_INCLUDE_PATHS = "PYTHON_INCLUDE_PATHS";

  public static final String PYTHON_SHARED_LIBS = "PYTHON_SHARED_LIBS";

  public transient Jep JEP_INSTANCE;

  private transient volatile boolean isStopped = false;

  private transient volatile boolean busyFlag = false;

  private long timeOutToPollFromRequestQueue = 1;

  private TimeUnit timeUnitsToPollFromRequestQueue = TimeUnit.MILLISECONDS;

  private transient volatile BlockingQueue<PythonRequestResponse> requestQueue;

  private transient volatile BlockingQueue<PythonRequestResponse> responseQueue;

  private String threadID;

  private Map<String,Object> initConfigs = new HashMap<>();

  public InterpreterThread(BlockingQueue<PythonRequestResponse> requestQueue,
      BlockingQueue<PythonRequestResponse> responseQueue,String threadID)
  {
    this.requestQueue = requestQueue;
    this.responseQueue = responseQueue;
    this.threadID = threadID;
  }

  private void loadMandatoryJVMLibraries() throws ApexPythonInterpreterException
  {
    LOG.info("Java library path being used for Interpreted ID " +  threadID + " " +
        System.getProperty("java.library.path"));
    System.loadLibrary(JEP_LIBRARY_NAME);
  }


  public Jep getEngineReference() throws ApexPythonInterpreterException
  {
    return JEP_INSTANCE;
  }

  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    initConfigs.putAll(preInitConfigs);
  }


  public void startInterpreter() throws ApexPythonInterpreterException
  {
    Thread.currentThread().setName(threadID);
    Thread.currentThread().setPriority(Thread.MAX_PRIORITY); // To allow for time aware calls
    loadMandatoryJVMLibraries();
    JepConfig config = new JepConfig()
        .setRedirectOutputStreams(true)
        .setInteractive(false)
        .setClassLoader(Thread.currentThread().getContextClassLoader()
    );
    if (initConfigs.containsKey(PYTHON_INCLUDE_PATHS)) {
      List<String> includePaths = (List<String>)initConfigs.get(PYTHON_INCLUDE_PATHS);
      if ( includePaths != null) {
        for (String anIncludePath: includePaths) {
          config.addIncludePaths(anIncludePath);
        }
      }
    }
    if (initConfigs.containsKey(PYTHON_SHARED_LIBS)) {
      Set<String> sharedLibs = (Set<String>)initConfigs.get(PYTHON_SHARED_LIBS);
      if ( sharedLibs != null) {
        config.setSharedModules(sharedLibs);
        LOG.info("Loaded " + sharedLibs.size() + " shared libraries as config");
      }
    } else {
      LOG.info("No shared libraries loaded");
    }
    try {
      JEP_INSTANCE = new Jep(config);
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }


  private Map<String,Boolean> runCommands(List<String> commands) throws ApexPythonInterpreterException
  {
    Map<String,Boolean> resultsOfExecution = new HashMap<>();
    for (String aCommand : commands) {
      try {
        resultsOfExecution.put(aCommand,JEP_INSTANCE.eval(aCommand));
      } catch (JepException e) {
        resultsOfExecution.put(aCommand,Boolean.FALSE);
        LOG.error("Error while running command " + aCommand, e);
        return resultsOfExecution;
      }
    }
    return resultsOfExecution;
  }

  private <T> T executeMethodCall(String nameOfGlobalMethod, List<Object> argsToGlobalMethod,
      Class<T> type) throws ApexPythonInterpreterException
  {
    try {
      if ((argsToGlobalMethod != null) && (argsToGlobalMethod.size() > 0)) {
        List<Object> paramsToPass = argsToGlobalMethod;
        List<Object> modifiedParams = new ArrayList<>();
        for ( Object aMethodParam: argsToGlobalMethod) {
          if (argsToGlobalMethod.get(0) instanceof NDimensionalArray) {
            modifiedParams.add(((NDimensionalArray)aMethodParam).toNDArray());
          } else {
            modifiedParams.add(aMethodParam);
          }
        }
        return type.cast(JEP_INSTANCE.invoke(nameOfGlobalMethod,modifiedParams.toArray()));
      } else {
        return type.cast(JEP_INSTANCE.invoke(nameOfGlobalMethod,new ArrayList<>().toArray()));
      }
    } catch (JepException e) {
      LOG.error("Error while executing method " + nameOfGlobalMethod, e);
    }
    return null;
  }

  private boolean executeScript(String scriptName)
    throws ApexPythonInterpreterException
  {
    try {
      JEP_INSTANCE.runScript(scriptName);
      return true;
    } catch (JepException e) {
      LOG.error(" Error while executing script " + scriptName, e);
    }
    return false;
  }

  private <T> T eval(String command, String variableToExtract, Map<String, Object> globalMethodsParams,
      boolean deleteExtractedVariable,Class<T> expectedReturnType) throws ApexPythonInterpreterException
  {
    T variableToReturn = null;
    LOG.debug(" params for eval passed in are " + command + " return type:" + expectedReturnType);
    try {
      for (String aKey : globalMethodsParams.keySet()) {
        Object keyVal = globalMethodsParams.get(aKey);
        if (keyVal instanceof NDimensionalArray) {
          keyVal = ((NDimensionalArray)keyVal).toNDArray();
        }
        JEP_INSTANCE.set(aKey, keyVal);
      }
    } catch (JepException e) {
      LOG.error("Error while setting the params for eval expression " + command, e);
      return null;
    }
    try {
      JEP_INSTANCE.eval(command);
    } catch (JepException e) {
      LOG.error("Error while evaluating the expression " + command, e);
      return null;
    }
    try {
      if (variableToExtract != null) {
        Object extractedVariable = JEP_INSTANCE.getValue(variableToExtract);
        if (extractedVariable instanceof NDArray) {
          NDArray ndArrayJepVal = (NDArray)extractedVariable;
          NDimensionalArray nDimArray = new NDimensionalArray();
          nDimArray.setData(ndArrayJepVal.getData());
          nDimArray.setSignedFlag(ndArrayJepVal.isUnsigned());
          int[] dimensions = ndArrayJepVal.getDimensions();
          nDimArray.setDimensions(dimensions);
          int lengthInOneDimension = 1;
          for ( int i=0; i < dimensions.length; i++) {
            lengthInOneDimension *= dimensions[i];
          }
          nDimArray.setLengthOfSequentialArray(lengthInOneDimension);
          variableToReturn = expectedReturnType.cast(nDimArray);
        } else {
          variableToReturn =  expectedReturnType.cast(extractedVariable);
        }
        if (deleteExtractedVariable) {
          JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + variableToExtract);
        }
      }
      for (String aKey: globalMethodsParams.keySet()) {
        LOG.debug("deleting " + aKey);
        JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + aKey);
      }
    } catch (JepException e) {
      LOG.error("Error while evaluating delete part of expression " + command, e);
      return null;
    }
    return variableToReturn;
  }

  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    isStopped = true;
    JEP_INSTANCE.close();
  }

  private <T> void processCommand() throws ApexPythonInterpreterException, InterruptedException
  {

    PythonRequestResponse requestResponseHandle = requestQueue.poll(timeOutToPollFromRequestQueue,
        timeUnitsToPollFromRequestQueue);
    if (requestResponseHandle != null) {
      LOG.debug("Processing command " + requestResponseHandle.getPythonInterpreterRequest().getCommandType());
      busyFlag = true;
      PythonInterpreterRequest<T> request =
          requestResponseHandle.getPythonInterpreterRequest();
      PythonInterpreterResponse<T> response =
          requestResponseHandle.getPythonInterpreterResponse();
      Map<String,Boolean> commandStatus = new HashMap<>(1);
      switch (request.getCommandType()) {
        case EVAL_COMMAND:
          EvalCommandRequestPayload evalPayload = request.getEvalCommandRequestPayload();
          T responseVal = eval(evalPayload.getEvalCommand(), evalPayload.getVariableNameToExtractInEvalCall(),
              evalPayload.getParamsForEvalCommand(), evalPayload.isDeleteVariableAfterEvalCall(),
              request.getExpectedReturnType());
          response.setResponse(responseVal);
          if (responseVal != null) {
            commandStatus.put(evalPayload.getEvalCommand(),Boolean.TRUE);
          } else {
            commandStatus.put(evalPayload.getEvalCommand(),Boolean.FALSE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case SCRIPT_COMMAND:
          ScriptExecutionRequestPayload scriptPayload = request.getScriptExecutionRequestPayload();
          if (executeScript(scriptPayload.getScriptName())) {
            commandStatus.put(scriptPayload.getScriptName(),Boolean.TRUE);
          } else {
            commandStatus.put(scriptPayload.getScriptName(),Boolean.FALSE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case METHOD_INVOCATION_COMMAND:
          MethodCallRequestPayload requestpayload = request.getMethodCallRequest();
          response.setResponse(executeMethodCall(
              requestpayload.getNameOfMethod(), requestpayload.getArgs(), request.getExpectedReturnType()));
          if (response.getResponse() == null) {
            commandStatus.put(requestpayload.getNameOfMethod(), Boolean.FALSE);
          } else {
            commandStatus.put(requestpayload.getNameOfMethod(), Boolean.TRUE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case GENERIC_COMMANDS:
          response.setCommandStatus(runCommands(request.getGenericCommandsRequestPayload().getGenericCommands()));
          break;
        default:
          throw new ApexPythonInterpreterException(new Exception("Unspecified Interpreter command"));
      }
      requestResponseHandle.setRequestCompletionTime(System.currentTimeMillis());
      responseQueue.put(requestResponseHandle);
      LOG.debug("Submitted the response " + response.getCommandStatus().size());
    }
    busyFlag = false;
  }

  @Override
  public void run()
  {
    LOG.info("Starting the execution of Interpreter thread " + threadID );
    if (JEP_INSTANCE == null) {
      try {
        startInterpreter();
      } catch (ApexPythonInterpreterException e) {
        throw new RuntimeException(e);
      }
    }
    while (!isStopped) {
      if (requestQueue.isEmpty()) {
        try {
          Thread.sleep(0L,100);
          continue;
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      try {
        processCommand();
      } catch (InterruptedException | ApexPythonInterpreterException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      stopInterpreter();
    } catch (ApexPythonInterpreterException e) {
      throw new RuntimeException(e);
    }
  }

  public Jep getJEP_INSTANCE()
  {
    return JEP_INSTANCE;
  }

  public void setJEP_INSTANCE(Jep JEP_INSTANCE)
  {
    this.JEP_INSTANCE = JEP_INSTANCE;
  }

  public long getTimeOutToPollFromRequestQueue()
  {
    return timeOutToPollFromRequestQueue;
  }

  public void setTimeOutToPollFromRequestQueue(long timeOutToPollFromRequestQueue)
  {
    this.timeOutToPollFromRequestQueue = timeOutToPollFromRequestQueue;
  }

  public TimeUnit getTimeUnitsToPollFromRequestQueue()
  {
    return timeUnitsToPollFromRequestQueue;
  }

  public void setTimeUnitsToPollFromRequestQueue(TimeUnit timeUnitsToPollFromRequestQueue)
  {
    this.timeUnitsToPollFromRequestQueue = timeUnitsToPollFromRequestQueue;
  }

  public boolean isStopped()
  {
    return isStopped;
  }

  public void setStopped(boolean stopped)
  {
    isStopped = stopped;
  }

  public BlockingQueue<PythonRequestResponse> getRequestQueue()
  {
    return requestQueue;
  }

  public void setRequestQueue(BlockingQueue<PythonRequestResponse> requestQueue)
  {
    this.requestQueue = requestQueue;
  }

  public BlockingQueue<PythonRequestResponse> getResponseQueue()
  {
    return responseQueue;
  }

  public void setResponseQueue(BlockingQueue<PythonRequestResponse> responseQueue)
  {
    this.responseQueue = responseQueue;
  }

  public Map<String, Object> getInitConfigs()
  {
    return initConfigs;
  }

  public void setInitConfigs(Map<String, Object> initConfigs)
  {
    this.initConfigs = initConfigs;
  }

  public boolean isBusy()
  {
    boolean busyState = busyFlag;
    if (!requestQueue.isEmpty()) { // This is required because interpreter thread goes to a 1 ms sleep to allow other
      //  threads work when checking the queue for request availability. Hence busy state flag need not necessarily
      // be updated in this sleep window even though if there is a pending request
      busyState = true;
    }
    return busyState;
  }

  public void setBusy(boolean busy)
  {
    busyFlag = busy;
  }
}
