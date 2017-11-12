package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

public class InterpreterWrapper
{
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterWrapper.class);

  private transient InterpreterThread interpreterThread;

  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 16; // Represents the number of workers and response queue sizes

  private int interpreterId;

  private transient BlockingQueue<PythonRequestResponse> requestQueue =
    new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private transient BlockingQueue<PythonRequestResponse> responseQueue =
    new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private transient BlockingQueue<PythonRequestResponse> delayedResponsesQueue;


  public InterpreterWrapper(int interpreterId,BlockingQueue<PythonRequestResponse> delayedResponsesQueueRef)
  {
    delayedResponsesQueue = delayedResponsesQueueRef;
    interpreterThread = new InterpreterThread(requestQueue,responseQueue);
  }

  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    interpreterThread.preInitInterpreter(preInitConfigs);
  }

  public void startInterpreter() throws ApexPythonInterpreterException
  {
    interpreterThread.startInterpreter();
  }

  private <T> PythonRequestResponse buildRequestObject(PythonRequestResponse.PythonCommandType commandType,
      long windowId,long requestId, Class<T> tClass)
  {
    PythonRequestResponse<T> requestResponse = new PythonRequestResponse();
    PythonRequestResponse<T>.PythonInterpreterRequest<T> request = requestResponse.new PythonInterpreterRequest<>();
    PythonRequestResponse<T>.PythonInterpreterResponse<T> response =
        requestResponse.new PythonInterpreterResponse<>(tClass);
    request.setCommandType(commandType);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(requestId);
    requestResponse.setWindowId(windowId);
    return requestResponse;
  }

  private PythonRequestResponse processRequest(PythonRequestResponse request, long timeout,
      TimeUnit timeUnit,List<PythonRequestResponse> stragglers) throws ApexPythonInterpreterException, TimeoutException
  {
    List<PythonRequestResponse> drainedResults = new ArrayList<>();
    PythonRequestResponse currentRequestWithResponse = null;
    // drain any previous responses that were returned while the Apex operator is processing
    responseQueue.drainTo(drainedResults);
    try {
      requestQueue.put(request);
      currentRequestWithResponse = responseQueue.poll(timeout,timeUnit); // ensures we are blocked till the time limit
      if (currentRequestWithResponse != null) {
        // add the last one that was just polled
        drainedResults.add(currentRequestWithResponse);
      }
      // possible that there are other responses not belonging to the current request that might have arrived
      responseQueue.drainTo(drainedResults);
      long currentRequestId = request.getRequestId();
      long currentWindowId = request.getWindowId();
      for(PythonRequestResponse aReqRespObject: drainedResults) {
        if ( (aReqRespObject.getWindowId() == currentWindowId) && (aReqRespObject.getRequestId() == currentRequestId)) {
          currentRequestWithResponse = aReqRespObject;
        } else {
          stragglers.add(aReqRespObject);
        }
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return currentRequestWithResponse;
  }

  public Map<String, Boolean> runCommands(long windowId, long requestId,
      List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException
  {
    List<PythonRequestResponse> stragglers = new ArrayList<>();
    PythonRequestResponse requestResponse = buildRequestObject(PythonRequestResponse.PythonCommandType.GENERIC_COMMANDS,
        windowId,requestId,Void.class);
    requestResponse.getPythonInterpreterRequest().setGenericCommands(commands);
    processRequest(requestResponse,timeout,timeUnit,stragglers);
    return null;
  }

  public <T> T executeMethodCall(long windowId, long requestId,
      String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit,
      Class<T> expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  public void executeScript(long windowId, long requestId, String scriptName,
      Map<String, Object> methodParams, long timeout, TimeUnit timeUnit)
      throws ApexPythonInterpreterException, TimeoutException
  {

  }

  public <T> T eval(long windowId, long requestId,String command,
      String variableNameToFetch, Map<String, Object> globalMethodsParams, long timeout, TimeUnit timeUnit,
      boolean deleteExtractedVariable, Class<T> expectedReturnType)
      throws ApexPythonInterpreterException,TimeoutException
  {
    return null;
  }

  public void stopInterpreter() throws ApexPythonInterpreterException
  {

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

  public int getInterpreterId()
  {
    return interpreterId;
  }

  public void setInterpreterId(int interpreterId)
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
}
