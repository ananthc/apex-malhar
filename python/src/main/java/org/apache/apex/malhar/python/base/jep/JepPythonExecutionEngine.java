package org.apache.apex.malhar.python.base.jep;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.AbstractApexPythonEngine;
import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

public class JepPythonExecutionEngine extends AbstractApexPythonEngine
{
  private static final Logger LOG = LoggerFactory.getLogger(JepPythonExecutionEngine.class);

  private transient JepPythonCommandExecutor jepPythonCommandExecutor;

  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 16; // Represents the number of workers and response queue sizes

  private int interpreterId;

  private transient BlockingQueue<PythonRequestResponse> requestQueue =
    new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private transient BlockingQueue<PythonRequestResponse> responseQueue =
    new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private transient BlockingQueue<PythonRequestResponse> delayedResponsesQueue;


  public JepPythonExecutionEngine(int interpreterId,BlockingQueue<PythonRequestResponse> delayedResponsesQueueRef)
  {
    delayedResponsesQueue = delayedResponsesQueueRef;
    jepPythonCommandExecutor = new JepPythonCommandExecutor(requestQueue,responseQueue);
  }

  @Override
  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    jepPythonCommandExecutor.preInitInterpreter(preInitConfigs);
  }

  @Override
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    jepPythonCommandExecutor.startInterpreter();
  }

  @Override
  public Map<String, Boolean> runCommands(WorkerExecutionMode executionMode,long windowId, long requestId,
      List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException
  {
    PythonRequestResponse requestResponse = new PythonRequestResponse();
    PythonRequestResponse.PythonInterpreterRequest request = requestResponse.new PythonInterpreterRequest<>();
    request.setCommandType(PythonRequestResponse.PythonCommandType.GENERIC_COMMANDS);
    request.setGenericCommands(commands);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(requestId);
    requestResponse.setWindowId(windowId);
    try {
      drain the responses if any
      requestQueue.put(requestResponse);
       possibility of the last one just eneterd the queue . Hence drain and comapre request id s
      PythonRequestResponse requestWithResponse = responseQueue.poll(timeout,timeUnit);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public <T> T executeMethodCall(WorkerExecutionMode executionMode, long windowId, long requestId,
      String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit,
      Class<T> expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
    return null;
  }

  @Override
  public void executeScript(WorkerExecutionMode executionMode, long windowId, long requestId, String scriptName,
      Map<String, Object> methodParams, long timeout, TimeUnit timeUnit)
      throws ApexPythonInterpreterException, TimeoutException
  {

  }

  @Override
  public <T> T eval(WorkerExecutionMode executionMode, long windowId, long requestId,String command,
      String variableNameToFetch, Map<String, Object> globalMethodsParams, long timeout, TimeUnit timeUnit,
      boolean deleteExtractedVariable, Class<T> expectedReturnType)
      throws ApexPythonInterpreterException,TimeoutException
  {
    return null;
  }

  @Override
  public void stopInterpreter() throws ApexPythonInterpreterException
  {

  }

  public JepPythonCommandExecutor getJepPythonCommandExecutor()
  {
    return jepPythonCommandExecutor;
  }

  public void setJepPythonCommandExecutor(JepPythonCommandExecutor jepPythonCommandExecutor)
  {
    this.jepPythonCommandExecutor = jepPythonCommandExecutor;
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
