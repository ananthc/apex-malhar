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
import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import com.google.common.primitives.Ints;

public class JepPythonEngine implements ApexPythonEngine
{
  private static final Logger LOG = LoggerFactory.getLogger(JepPythonEngine.class);

  private int numWorkerThreads = 3;

  private String threadGroupName;

  private static final String JEP_LIBRARY_NAME = "jep";

  private transient List<PythonRequestResponse> commandHistory = new ArrayList<>();

  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 64; // Represents the number of workers and response queue sizes

  private transient BlockingQueue<PythonRequestResponse> delayedResponseQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private List<InterpreterWrapper> workers = new ArrayList<>();

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
      aWorker.startInterpreter();
      workers.add(aWorker);
    }
  }

  protected InterpreterWrapper selectWorkerForCurrentCall(long requestId)
  {
    int slotToLookFor = Ints.saturatedCast(requestId) % numWorkerThreads;
    boolean isWorkerFound = false;
    int numWorkersScannedForAvailability = 0;
    InterpreterWrapper aWorker = null;
    while ( (!isWorkerFound) && (numWorkersScannedForAvailability < numWorkerThreads)) {
      aWorker = workers.get(slotToLookFor);
      numWorkersScannedForAvailability  = numWorkersScannedForAvailability + 1;
      if (!aWorker.isCurrentlyBusy()) {
        isWorkerFound = true;
        break;
      } else {
        slotToLookFor = slotToLookFor + 1;
        if ( slotToLookFor == numWorkerThreads) {
          slotToLookFor = 0;
        }
      }
    }
    if (isWorkerFound) {
      return aWorker;
    } else {
      return null;
    }
  }

  @Override
  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    for ( InterpreterWrapper wrapper : workers) {
      wrapper.preInitInterpreter(preInitConfigs);
    }
  }

  @Override
  public void startInterpreter() throws ApexPythonInterpreterException
  {
    initWorkers();
  }

  @Override
  public void postStartInterpreter() throws ApexPythonInterpreterException
  {
    for ( InterpreterWrapper wrapper : workers) {
      for (PythonRequestResponse requestResponse : commandHistory) {
        PythonRequestResponse.PythonInterpreterRequest requestPayload = requestResponse.getPythonInterpreterRequest();
        wrapper.processRequest(requestResponse,requestPayload.getTimeOutInNanos(), TimeUnit.NANOSECONDS,
            requestPayload.getExpectedReturnType());
      }
    }
  }

  @Override
  public Map<String,PythonRequestResponse>  runCommands(WorkerExecutionMode executionMode,long windowId,long requestId,
      List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException
  {
    Map<String,PythonRequestResponse> returnStatus = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    if (!executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
      InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
      if ( currentThread != null) {
        lastSuccessfullySubmittedRequest = currentThread.runCommands(windowId,requestId,commands,timeout,timeUnit);
        returnStatus.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
      } else {
        throw new ApexPythonInterpreterException("No free interpreter threads available." +
          " Consider increasing workers and relaunch");
      }
    } else {
      long timeOutPerWorker = timeout / numWorkerThreads;
      if ( timeOutPerWorker == 0) {
        timeOutPerWorker = 1;
      }
      for ( InterpreterWrapper wrapper : workers) {
        lastSuccessfullySubmittedRequest = wrapper.runCommands(windowId,requestId,commands,timeOutPerWorker,timeUnit);
        returnStatus.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
      }
    }
    if ( returnStatus.size() > 0) {
      commandHistory.add(lastSuccessfullySubmittedRequest);
    }
    return returnStatus;
  }

  @Override
  public <T> Map<String,PythonRequestResponse<T>> executeMethodCall(WorkerExecutionMode executionMode,long windowId,
      long requestId, String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit,
      Class<T> expectedReturnType) throws ApexPythonInterpreterException
  {
    Map<String,PythonRequestResponse<T>> returnStatus = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    if (executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
      for ( InterpreterWrapper wrapper : workers) {
        lastSuccessfullySubmittedRequest = wrapper.executeMethodCall(windowId,requestId,nameOfGlobalMethod,
          argsToGlobalMethod, timeout,timeUnit,expectedReturnType);
        returnStatus.put(wrapper.getInterpreterId(),lastSuccessfullySubmittedRequest);
      }
    } else {
      InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
      if ( currentThread != null) {
        lastSuccessfullySubmittedRequest = currentThread.executeMethodCall(windowId,requestId,
          nameOfGlobalMethod,argsToGlobalMethod,timeout,timeUnit,expectedReturnType);
        returnStatus.put(currentThread.getInterpreterId(),lastSuccessfullySubmittedRequest);
      } else {
        throw new ApexPythonInterpreterException("No free interpreter threads available." +
          " Consider increasing workers and relaunch");
      }
    }
    if ( returnStatus.size() > 0) {
      commandHistory.add(lastSuccessfullySubmittedRequest);
    }
    return returnStatus;
  }

  @Override
  public Map<String,PythonRequestResponse>  executeScript(WorkerExecutionMode executionMode,long windowId,
      long requestId, String scriptName,long timeout, TimeUnit timeUnit)
    throws ApexPythonInterpreterException
  {
    Map<String,PythonRequestResponse> returnStatus = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    if (executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
      for ( InterpreterWrapper wrapper : workers) {
        lastSuccessfullySubmittedRequest = wrapper.executeScript(windowId,requestId,scriptName,
          timeout,timeUnit);
        returnStatus.put(wrapper.getInterpreterId(),lastSuccessfullySubmittedRequest);
      }
    } else {
      InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
      if (currentThread != null) {
        lastSuccessfullySubmittedRequest = currentThread.executeScript(windowId, requestId, scriptName,
          timeout, timeUnit);
        returnStatus.put(currentThread.getInterpreterId(), lastSuccessfullySubmittedRequest);
      } else {
        throw new ApexPythonInterpreterException("No free interpreter threads available." +
          " Consider increasing workers and relaunch");
      }
    }
    if ( returnStatus.size() > 0) {
      commandHistory.add(lastSuccessfullySubmittedRequest);
    }
    return returnStatus;
  }

  @Override
  public <T> Map<String,PythonRequestResponse<T>> eval(WorkerExecutionMode executionMode,long windowId, long requestId,
      String command, String variableNameToFetch,Map<String, Object> globalMethodsParams,long timeout,TimeUnit timeUnit,
      boolean deleteExtractedVariable,Class<T> expectedReturnType)
    throws ApexPythonInterpreterException
  {
    Map<String,PythonRequestResponse<T>> statusOfEval = new HashMap<>();
    PythonRequestResponse lastSuccessfullySubmittedRequest = null;
    if (!executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
      InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
      if ( currentThread != null) {
        lastSuccessfullySubmittedRequest = currentThread.eval(windowId,requestId,command,variableNameToFetch,
          globalMethodsParams, timeout,timeUnit, deleteExtractedVariable,expectedReturnType);
        statusOfEval.put(currentThread.getInterpreterId(),lastSuccessfullySubmittedRequest);
      } else {
        throw new ApexPythonInterpreterException("No free interpreter threads available." +
          " Consider increasing workers and relaunch");
      }
    } else {
      long timeOutPerWorker = timeout / numWorkerThreads;
      if ( timeOutPerWorker == 0) {
        timeOutPerWorker = 1;
      }
      for ( InterpreterWrapper wrapper : workers) {
        lastSuccessfullySubmittedRequest = wrapper.eval(windowId,requestId,command,variableNameToFetch,
          globalMethodsParams, timeOutPerWorker,timeUnit, deleteExtractedVariable,expectedReturnType);
        statusOfEval.put(wrapper.getInterpreterId(), lastSuccessfullySubmittedRequest);
      }
    }
    if ( statusOfEval.size() > 0) {
      commandHistory.add(lastSuccessfullySubmittedRequest);
    }
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

  public List<PythonRequestResponse> getCommandHistory()
  {
    return commandHistory;
  }

  public void setCommandHistory(List<PythonRequestResponse> commandHistory)
  {
    this.commandHistory = commandHistory;
  }
}
