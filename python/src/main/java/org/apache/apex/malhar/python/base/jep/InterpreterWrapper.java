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
import org.apache.apex.malhar.python.base.requestresponse.EvalCommandRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.GenericCommandsRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.MethodCallRequestPayload;
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

  private <T> PythonRequestResponse<T> buildRequestObject(PythonCommandType commandType,
      long windowId,long requestId, Class<T> tClass) throws ApexPythonInterpreterException
  {
    PythonRequestResponse<T> requestResponse = new PythonRequestResponse();
    PythonInterpreterRequest<T> request = new PythonInterpreterRequest<>();
    requestResponse.setPythonInterpreterRequest(request);
    PythonInterpreterResponse<T> response = new PythonInterpreterResponse<>(tClass);
    requestResponse.setPythonInterpreterResponse(response);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    request.setCommandType(commandType);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(requestId);
    requestResponse.setWindowId(windowId);
    switch (commandType) {
      case METHOD_INVOCATION_COMMAND:
        MethodCallRequestPayload methodCallRequest = new MethodCallRequestPayload();
        request.setMethodCallRequest(methodCallRequest);
        break;
      case GENERIC_COMMANDS:
        GenericCommandsRequestPayload genericPayload = new GenericCommandsRequestPayload();
        request.setGenericCommandsRequestPayload(genericPayload);
        break;
      case EVAL_COMMAND:
        EvalCommandRequestPayload evalPayload = new EvalCommandRequestPayload();
        request.setEvalCommandRequestPayload(evalPayload);
        break;
      default:
        throw new ApexPythonInterpreterException("Unsupported command type");
    }
    return requestResponse;
  }

  public <T> PythonRequestResponse<T> processRequest(PythonRequestResponse request, long timeout, TimeUnit timeUnit,
      Class<T> clazz) throws ApexPythonInterpreterException
  {
    List<PythonRequestResponse> drainedResults = new ArrayList<>();
    PythonRequestResponse currentRequestWithResponse = null;
    boolean isCurrentRequestProcessed = false;
    long timeOutInNanos = TimeUnit.NANOSECONDS.convert(timeout,timeUnit);
    request.getPythonInterpreterRequest().setTimeOutInNanos(timeOutInNanos); // To be used in command history invocation
    request.getPythonInterpreterRequest().setExpectedReturnType(clazz);
    // drain any previous responses that were returned while the Apex operator is processing
    responseQueue.drainTo(drainedResults);
    LOG.debug("Draining previous request responses if any " + drainedResults.size());
    try {
      for (PythonRequestResponse requestResponse : drainedResults) {
        delayedResponsesQueue.put(requestResponse);
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
        requestQueue.put(request);
        // ensures we are blocked till the time limit
        currentRequestWithResponse = responseQueue.poll(timeOutInNanos, TimeUnit.NANOSECONDS);
        timeLeftToCompleteProcessing = timeLeftToCompleteProcessing - ( System.nanoTime() - currentStart );
        currentStart = System.nanoTime();
        if (currentRequestWithResponse != null) {
          if ( (request.getRequestId() == currentRequestWithResponse.getRequestId()) &&
              (request.getWindowId() == currentRequestWithResponse.getWindowId()) ) {
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

  public PythonRequestResponse runCommands(long windowId, long requestId,
      List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException
  {
    PythonRequestResponse requestResponse = buildRequestObject(PythonCommandType.GENERIC_COMMANDS,
        windowId,requestId,Void.class);
    requestResponse.getPythonInterpreterRequest().getGenericCommandsRequestPayload().setGenericCommands(commands);
    return processRequest(requestResponse,timeout,timeUnit,Void.class);
  }

  public <T> PythonRequestResponse<T> executeMethodCall(long windowId, long requestId,
      String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit,
      Class<T> expectedReturnType)
    throws ApexPythonInterpreterException
  {
    PythonRequestResponse requestResponse = buildRequestObject(
        PythonCommandType.METHOD_INVOCATION_COMMAND,
        windowId,requestId,expectedReturnType);
    requestResponse.getPythonInterpreterRequest().getMethodCallRequest().setNameOfMethod(
        nameOfGlobalMethod);
    requestResponse.getPythonInterpreterRequest().getMethodCallRequest().setArgs(
        argsToGlobalMethod);
    return processRequest(requestResponse,timeout,timeUnit, expectedReturnType);
  }

  public PythonRequestResponse executeScript(long windowId, long requestId, String scriptName,
      long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException
  {
    PythonRequestResponse<Void> requestResponse = buildRequestObject(
        PythonCommandType.SCRIPT_COMMAND, windowId,requestId,Void.class);
    PythonInterpreterRequest<Void> request = requestResponse.getPythonInterpreterRequest();
    request.getScriptExecutionRequestPayload().setScriptName(scriptName);
    return processRequest(requestResponse,timeout,timeUnit,Void.class);
  }

  public <T> PythonRequestResponse<T> eval(long windowId, long requestId,String command,
      String variableNameToFetch, Map<String, Object> paramsForEval, long timeout, TimeUnit timeUnit,
      boolean deleteExtractedVariable, Class<T> expectedReturnType)
    throws ApexPythonInterpreterException
  {
    PythonRequestResponse<T> requestResponse = buildRequestObject(
        PythonCommandType.EVAL_COMMAND, windowId,requestId,expectedReturnType);
    PythonInterpreterRequest<T> request = requestResponse.getPythonInterpreterRequest();
    EvalCommandRequestPayload evalCommandRequestPayload = request.getEvalCommandRequestPayload();
    evalCommandRequestPayload.setEvalCommand(command);
    evalCommandRequestPayload.setVariableNameToExtractInEvalCall(variableNameToFetch);
    evalCommandRequestPayload.setDeleteVariableAfterEvalCall(deleteExtractedVariable);
    evalCommandRequestPayload.setParamsForEvalCommand(paramsForEval);
    return processRequest(requestResponse,timeout,timeUnit, expectedReturnType);
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
