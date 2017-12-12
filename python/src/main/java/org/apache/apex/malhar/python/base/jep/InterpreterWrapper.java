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
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.requestresponse.PythonCommandType;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterResponse;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;


public class InterpreterWrapper
{
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterWrapper.class);

  private transient InterpreterThread interpreterThread;

  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 16; // Represents the number of workers and response queue sizes

  private String interpreterId;

  private Future<?> handleToJepRunner;

  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  private transient BlockingQueue<PythonRequestResponse> requestQueue;
  private transient BlockingQueue<PythonRequestResponse> responseQueue;

  private transient volatile BlockingQueue<PythonRequestResponse> delayedResponsesQueue;


  public InterpreterWrapper(String interpreterId,BlockingQueue<PythonRequestResponse> delayedResponsesQueueRef)
  {
    delayedResponsesQueue = delayedResponsesQueueRef;
    this.interpreterId = interpreterId;
    requestQueue = new DisruptorBlockingQueue<>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);
    responseQueue =  new DisruptorBlockingQueue<>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);
    interpreterThread = new InterpreterThread(requestQueue,responseQueue,interpreterId);
  }

  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    interpreterThread.preInitInterpreter(preInitConfigs);
  }

  public void startInterpreter() throws ApexPythonInterpreterException
  {
    handleToJepRunner = executorService.submit(interpreterThread);
  }

  private <T> PythonRequestResponse<T> buildRequestRespObject(PythonInterpreterRequest<T> req,
      long windowId,long requestId) throws ApexPythonInterpreterException
  {
    PythonRequestResponse<T> requestResponse = new PythonRequestResponse();
    requestResponse.setPythonInterpreterRequest(req);
    PythonInterpreterResponse<T> response = new PythonInterpreterResponse<>(req.getExpectedReturnType());
    requestResponse.setPythonInterpreterResponse(response);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(requestId);
    requestResponse.setWindowId(windowId);
    return requestResponse;
  }

  public <T> PythonRequestResponse<T> processRequest(PythonRequestResponse requestResponse,
      PythonInterpreterRequest<T> req) throws ApexPythonInterpreterException
  {
    List<PythonRequestResponse> drainedResults = new ArrayList<>();
    PythonRequestResponse currentRequestWithResponse = null;
    boolean isCurrentRequestProcessed = false;
    long timeOutInNanos = TimeUnit.NANOSECONDS.convert(req.getTimeout(),req.getTimeUnit());
    // drain any previous responses that were returned while the Apex operator is processing
    responseQueue.drainTo(drainedResults);
    LOG.debug("Draining previous request responses if any " + drainedResults.size());
    try {
      for (PythonRequestResponse oldRequestResponse : drainedResults) {
        delayedResponsesQueue.put(oldRequestResponse);
      }
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
    // We first set a timer to see how long it actually it took for the response to arrive.
    // It is possible that a response arrived due to a previous request and hence this need for the timer
    // which tracks the time for the current request.
    long currentStart = System.nanoTime();
    long timeLeftToCompleteProcessing = timeOutInNanos;
    while ( (!isCurrentRequestProcessed) && ( timeLeftToCompleteProcessing > 0 )) {
      try {
        LOG.debug("Submitting the interpreter Request with time out in nanos as " + timeOutInNanos);
        requestQueue.put(requestResponse);
        // ensures we are blocked till the time limit
        currentRequestWithResponse = responseQueue.poll(timeOutInNanos, TimeUnit.NANOSECONDS);
        timeLeftToCompleteProcessing = timeLeftToCompleteProcessing - ( System.nanoTime() - currentStart );
        currentStart = System.nanoTime();
        if (currentRequestWithResponse != null) {
          if ( (requestResponse.getRequestId() == currentRequestWithResponse.getRequestId()) &&
              (requestResponse.getWindowId() == currentRequestWithResponse.getWindowId()) ) {
            isCurrentRequestProcessed = true;
            break;
          } else {
            delayedResponsesQueue.put(currentRequestWithResponse);
          }
        } else {
          LOG.debug(" Processing of request could not be completed on time");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if (isCurrentRequestProcessed) {
      return currentRequestWithResponse;
    } else {
      return null;
    }
  }

  public PythonRequestResponse<Void> runCommands(long windowId, long requestId,
      PythonInterpreterRequest<Void> request) throws ApexPythonInterpreterException
  {
    request.setCommandType(PythonCommandType.GENERIC_COMMANDS);
    PythonRequestResponse requestResponse = buildRequestRespObject(request,windowId,requestId);
    return processRequest(requestResponse,request);
  }

  public <T> PythonRequestResponse<T> executeMethodCall(long windowId, long requestId,
      PythonInterpreterRequest<T> request)
    throws ApexPythonInterpreterException
  {
    request.setCommandType(PythonCommandType.METHOD_INVOCATION_COMMAND);
    PythonRequestResponse requestResponse = buildRequestRespObject(request, windowId,requestId);
    return processRequest(requestResponse,request);
  }

  public PythonRequestResponse<Void> executeScript(long windowId,long requestId,PythonInterpreterRequest<Void> request)
    throws ApexPythonInterpreterException
  {
    request.setCommandType(PythonCommandType.SCRIPT_COMMAND);
    PythonRequestResponse<Void> requestResponse = buildRequestRespObject(request, windowId,requestId);
    return processRequest(requestResponse,request);
  }

  public <T> PythonRequestResponse<T> eval(long windowId, long requestId,PythonInterpreterRequest<T> request)
    throws ApexPythonInterpreterException
  {
    request.setCommandType(PythonCommandType.EVAL_COMMAND);
    PythonRequestResponse<T> requestResponse = buildRequestRespObject(request,windowId,requestId);
    return processRequest(requestResponse,request);
  }

  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    interpreterThread.setStopped(true);
    handleToJepRunner.cancel(false);
    executorService.shutdown();
  }

  public InterpreterThread getInterpreterThread()
  {
    return interpreterThread;
  }

  public void setInterpreterThread(InterpreterThread interpreterThread)
  {
    this.interpreterThread = interpreterThread;
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

  public String getInterpreterId()
  {
    return interpreterId;
  }

  public void setInterpreterId(String interpreterId)
  {
    this.interpreterId = interpreterId;
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

  public BlockingQueue<PythonRequestResponse> getDelayedResponsesQueue()
  {
    return delayedResponsesQueue;
  }

  public void setDelayedResponsesQueue(BlockingQueue<PythonRequestResponse> delayedResponsesQueue)
  {
    this.delayedResponsesQueue = delayedResponsesQueue;
  }

  public boolean isCurrentlyBusy()
  {
    return interpreterThread.isBusy();
  }


}
