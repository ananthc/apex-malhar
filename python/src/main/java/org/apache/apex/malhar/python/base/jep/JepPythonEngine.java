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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonEngine;
import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;
import org.apache.apex.malhar.python.base.requestresponse.PythonCommandType;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import com.google.common.primitives.Ints;

import static com.google.common.base.Preconditions.checkNotNull;

public class JepPythonEngine implements ApexPythonEngine
{
  private static final Logger LOG = LoggerFactory.getLogger(JepPythonEngine.class);

  private int numWorkerThreads = 2;

  private String threadGroupName;

  private static final String JEP_LIBRARY_NAME = "jep";

  private transient List<PythonRequestResponse> commandHistory = new ArrayList<>();

  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 64; // Represents the number of workers and response queue sizes

  private long sleepTimeAfterInterpreterStart = 2000; // 2 secs

  private transient BlockingQueue<PythonRequestResponse> delayedResponseQueue =
      new DisruptorBlockingQueue<>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private List<InterpreterWrapper> workers = new ArrayList<>();

  private Map<String, Object> preInitConfigs;

  private long numStarvedReturns = 0;

  public JepPythonEngine(String threadGroupName,int numWorkerThreads)
  {
    this.numWorkerThreads = numWorkerThreads;
    this.threadGroupName = threadGroupName;
  }

  private void initWorkers() throws ApexPythonInterpreterException
  {
    System.loadLibrary(JEP_LIBRARY_NAME);
    for ( int i = 0; i < numWorkerThreads; i++) {
      InterpreterWrapper aWorker = new InterpreterWrapper(threadGroupName + "-" + i,delayedResponseQueue);
      aWorker.preInitInterpreter(preInitConfigs);
      aWorker.startInterpreter();
      workers.add(aWorker);
    }
  }

  protected InterpreterWrapper selectWorkerForCurrentCall(long requestId)
  {
    int slotToLookFor = Ints.saturatedCast(requestId) % numWorkerThreads;
    LOG.debug("Slot that is being looked for in the worker pool " + slotToLookFor);
    boolean isWorkerFound = false;
    int numWorkersScannedForAvailability = 0;
    InterpreterWrapper aWorker = null;
    while ( (!isWorkerFound) && (numWorkersScannedForAvailability < numWorkerThreads)) {
      aWorker = workers.get(slotToLookFor);
      numWorkersScannedForAvailability  = numWorkersScannedForAvailability + 1;
      if (!aWorker.isCurrentlyBusy()) {
        isWorkerFound = true;
        LOG.debug("Found worker with index as  " + slotToLookFor);
        break;
      } else {
        LOG.debug("Thread ID is currently busy " + aWorker.getInterpreterId());
        slotToLookFor = slotToLookFor + 1;
        if ( slotToLookFor == numWorkerThreads) {
          slotToLookFor = 0;
        }
      }
    }
    if (isWorkerFound) {
      return aWorker;
    } else {
      numStarvedReturns += 1;
      return null;
    }
  }

  @Override
  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    this.preInitConfigs = preInitConfigs;
  }

  @Override
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    initWorkers();
    try {
      LOG.debug("Sleeping to let the interpreter boot up in memory");
      Thread.sleep(sleepTimeAfterInterpreterStart);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void postStartInterpreter() throws ApexPythonInterpreterException
  {
    for ( InterpreterWrapper wrapper : workers) {
      for (PythonRequestResponse requestResponse : commandHistory) {
        PythonInterpreterRequest requestPayload = requestResponse.getPythonInterpreterRequest();
        try {
          wrapper.processRequest(requestResponse,requestPayload);
        } catch (InterruptedException e) {
          throw new ApexPythonInterpreterException(e);
        }
      }
    }
  }

  @Override
  public Map<String,PythonRequestResponse<Void>> runCommands(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<Void> request) throws ApexPythonInterpreterException
  {
    checkNotNullConditions(request);
    checkNotNull(request.getGenericCommandsRequestPayload(), "Run commands payload not set");
    checkNotNull(request.getGenericCommandsRequestPayload().getGenericCommands(),
        "Commands that need to be run not set");
    Map<String,PythonRequestResponse<Void>> returnStatus = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (!executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
        LOG.debug("Executing run commands on a single interpreter worker thread");
        InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
        if ( currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.runCommands(windowId,requestId,request);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        } else {
          throw new ApexPythonInterpreterException("No free interpreter threads available." +
            " Consider increasing workers and relaunch");
        }
      } else {
        LOG.debug("Executing run commands on all of the interpreter worker threads");
        long  timeOutPerWorker = TimeUnit.NANOSECONDS.convert(request.getTimeout(),request.getTimeUnit()) /
            numWorkerThreads;
        LOG.debug("Allocating " + timeOutPerWorker + " nanoseconds for each of the worker threads");
        if ( timeOutPerWorker == 0) {
          timeOutPerWorker = 1;
        }
        request.setTimeout(timeOutPerWorker);
        request.setTimeUnit(TimeUnit.NANOSECONDS);
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.runCommands(windowId,requestId,request);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    if ( returnStatus.size() > 0) {
      commandHistory.add(lastSuccessfullySubmittedRequest);
    }
    return returnStatus;
  }

  @Override
  public <T> Map<String,PythonRequestResponse<T>> executeMethodCall(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<T> req) throws ApexPythonInterpreterException
  {
    checkNotNullConditions(req);
    checkNotNull(req.getMethodCallRequest(), "Method call info not set");
    checkNotNull(req.getMethodCallRequest().getNameOfMethod(), "Method name not set");
    Map<String,PythonRequestResponse<T>> returnStatus = new HashMap<>();
    req.setCommandType(PythonCommandType.METHOD_INVOCATION_COMMAND);
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.executeMethodCall(windowId,requestId,req);
          if ( lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        }
      } else {
        InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
        if ( currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.executeMethodCall(windowId,requestId,req);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(currentThread.getInterpreterId(),lastSuccessfullySubmittedRequest);
          }
        } else {
          throw new ApexPythonInterpreterException("No free interpreter threads available." +
            " Consider increasing workers and relaunch");
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    if ( returnStatus.size() > 0) {
      commandHistory.add(lastSuccessfullySubmittedRequest);
    }
    return returnStatus;
  }

  @Override
  public Map<String,PythonRequestResponse<Void>> executeScript(WorkerExecutionMode executionMode,long windowId,
      long requestId, PythonInterpreterRequest<Void> req)
    throws ApexPythonInterpreterException
  {
    checkNotNullConditions(req);
    checkNotNull(req.getScriptExecutionRequestPayload(), "Script execution info not set");
    checkNotNull(req.getScriptExecutionRequestPayload().getScriptName(), "Script name not set");
    Map<String,PythonRequestResponse<Void>> returnStatus = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.executeScript(windowId,requestId,req);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(wrapper.getInterpreterId(),lastSuccessfullySubmittedRequest);
          }
        }
      } else {
        InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
        if (currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.executeScript(windowId, requestId,req);
          if (lastSuccessfullySubmittedRequest != null) {
            returnStatus.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        } else {
          throw new ApexPythonInterpreterException("No free interpreter threads available." +
            " Consider increasing workers and relaunch");
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    if ( returnStatus.size() > 0) {
      commandHistory.add(lastSuccessfullySubmittedRequest);
    }
    return returnStatus;
  }

  private void checkNotNullConditions(PythonInterpreterRequest request)
  {
    checkNotNull(request, "Request object cannnot be null");
    checkNotNull(request.getTimeout(), "Time out value not set");
    checkNotNull(request.getTimeUnit(), "Time out unit not set");
  }

  @Override
  public <T> Map<String,PythonRequestResponse<T>> eval(WorkerExecutionMode executionMode,long windowId, long requestId,
      PythonInterpreterRequest<T> request) throws ApexPythonInterpreterException
  {
    checkNotNullConditions(request);
    checkNotNull(request.getEvalCommandRequestPayload(), "Eval command info not set");
    checkNotNull(request.getEvalCommandRequestPayload().getEvalCommand(),"Eval command not set");
    Map<String,PythonRequestResponse<T>> statusOfEval = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    try {
      if (!executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
        InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
        if ( currentThread != null) {
          lastSuccessfullySubmittedRequest = currentThread.eval(windowId,requestId,request);
          if ( lastSuccessfullySubmittedRequest != null) {
            statusOfEval.put(currentThread.getInterpreterId(),lastSuccessfullySubmittedRequest);
          }
        } else {
          throw new ApexPythonInterpreterException("No free interpreter threads available." +
            " Consider increasing workers and relaunch");
        }
      } else {
        long timeOutPerWorker = TimeUnit.NANOSECONDS.convert(request.getTimeout(), request.getTimeUnit()) /
            numWorkerThreads;
        if ( timeOutPerWorker == 0) {
          timeOutPerWorker = 1;
        }
        request.setTimeout(timeOutPerWorker);
        request.setTimeUnit(TimeUnit.NANOSECONDS);
        for ( InterpreterWrapper wrapper : workers) {
          lastSuccessfullySubmittedRequest = wrapper.eval(windowId,requestId,request);
          if (lastSuccessfullySubmittedRequest != null) {
            statusOfEval.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
          }
        }
      }
    } catch (InterruptedException e) {
      throw new ApexPythonInterpreterException(e);
    }
    commandHistory.add(lastSuccessfullySubmittedRequest);
    return statusOfEval;
  }

  @Override
  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    for ( InterpreterWrapper wrapper : workers) {
      wrapper.stopInterpreter();
    }
  }

  public int getNumWorkerThreads()
  {
    return numWorkerThreads;
  }

  public void setNumWorkerThreads(int numWorkerThreads)
  {
    this.numWorkerThreads = numWorkerThreads;
  }

  public List<InterpreterWrapper> getWorkers()
  {
    return workers;
  }

  public void setWorkers(List<InterpreterWrapper> workers)
  {
    this.workers = workers;
  }

  @Override
  public List<PythonRequestResponse> getCommandHistory()
  {
    return commandHistory;
  }

  @Override
  public void setCommandHistory(List<PythonRequestResponse> commandHistory)
  {
    this.commandHistory = commandHistory;
  }

  public long getSleepTimeAfterInterpreterStart()
  {
    return sleepTimeAfterInterpreterStart;
  }

  public void setSleepTimeAfterInterpreterStart(long sleepTimeAfterInterpreterStart)
  {
    this.sleepTimeAfterInterpreterStart = sleepTimeAfterInterpreterStart;
  }

  @Override
  public BlockingQueue<PythonRequestResponse> getDelayedResponseQueue()
  {
    return delayedResponseQueue;
  }

  @Override
  public void setDelayedResponseQueue(BlockingQueue<PythonRequestResponse> delayedResponseQueue)
  {
    this.delayedResponseQueue = delayedResponseQueue;
  }

  public SpinPolicy getCpuSpinPolicyForWaitingInBuffer()
  {
    return cpuSpinPolicyForWaitingInBuffer;
  }

  public void setCpuSpinPolicyForWaitingInBuffer(SpinPolicy cpuSpinPolicyForWaitingInBuffer)
  {
    this.cpuSpinPolicyForWaitingInBuffer = cpuSpinPolicyForWaitingInBuffer;
  }

  public int getBufferCapacity()
  {
    return bufferCapacity;
  }

  public void setBufferCapacity(int bufferCapacity)
  {
    this.bufferCapacity = bufferCapacity;
  }

  @Override
  public long getNumStarvedReturns()
  {
    return numStarvedReturns;
  }

  @Override
  public void setNumStarvedReturns(long numStarvedReturns)
  {
    this.numStarvedReturns = numStarvedReturns;
  }
}
