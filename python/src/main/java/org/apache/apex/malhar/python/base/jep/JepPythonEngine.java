package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonEngine;
import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;
import com.google.common.primitives.Ints;

public abstract class JepPythonEngine implements ApexPythonEngine
{
  private static final Logger LOG = LoggerFactory.getLogger(JepPythonEngine.class);

  private int numWorkerThreads = 3;

  private transient SpinPolicy cpuSpinPolicyForWaitingInBuffer = SpinPolicy.WAITING;

  private int bufferCapacity = 64; // Represents the number of workers and response queue sizes

  private transient BlockingQueue<PythonRequestResponse> delayedResponseQueue =
    new DisruptorBlockingQueue<PythonRequestResponse>(bufferCapacity,cpuSpinPolicyForWaitingInBuffer);

  private List<InterpreterWrapper> workers = new ArrayList<>();

  public JepPythonEngine(int numWorkerThreads)
  {
    this.numWorkerThreads = numWorkerThreads;
    initWorkers();
  }

  public JepPythonEngine()
  {
    initWorkers();
  }

  private void initWorkers()
  {
    for ( int i=0; i < numWorkerThreads; i++) {
      InterpreterWrapper aWorker = new InterpreterWrapper(i,delayedResponseQueue);
      workers.add(aWorker);
    }
  }

  private InterpreterWrapper selectWorkerForCurrentCall(long requestId)
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
    for ( InterpreterWrapper wrapper : workers) {
      wrapper.startInterpreter();
    }
  }

  @Override
  public void runCommands(WorkerExecutionMode executionMode, long windowId, long requestId,
      List<String> commands, long timeout, TimeUnit timeUnit) throws ApexPythonInterpreterException, TimeoutException
  {
    if (!executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
      InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
      if ( currentThread != null) {
        currentThread.runCommands(windowId,requestId,commands,timeout,timeUnit);
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
        wrapper.runCommands(windowId,requestId,commands,timeOutPerWorker,timeUnit);
      }
    }
  }

  @Override
  public <T> PythonRequestResponse<T> executeMethodCall(long windowId, long requestId,
      String nameOfGlobalMethod, List<Object> argsToGlobalMethod, long timeout, TimeUnit timeUnit,
      Class<T> expectedReturnType) throws ApexPythonInterpreterException, TimeoutException
  {
      InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
      if ( currentThread != null) {
        return currentThread.executeMethodCall(windowId,requestId,nameOfGlobalMethod,argsToGlobalMethod,
            timeout,timeUnit,expectedReturnType);
      } else {
        throw new ApexPythonInterpreterException("No free interpreter threads available." +
          " Consider increasing workers and relaunch");
      }
  }

  @Override
  public void executeScript(long windowId, long requestId, String scriptName,
      Map<String, Object> scriptParams, long timeout, TimeUnit timeUnit)
      throws ApexPythonInterpreterException,TimeoutException
  {
    InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
    if ( currentThread != null) {
      currentThread.executeScript(windowId,requestId,scriptName,scriptParams,
        timeout,timeUnit);
    } else {
      throw new ApexPythonInterpreterException("No free interpreter threads available." +
        " Consider increasing workers and relaunch");
    }
  }

  @Override
  public <T> Map<Integer,PythonRequestResponse<T>> eval(WorkerExecutionMode executionMode, long windowId, long requestId,
      String command, String variableNameToFetch,Map<String, Object> globalMethodsParams,long timeout,TimeUnit timeUnit,
      boolean deleteExtractedVariable, Class<T> expectedReturnType)
      throws ApexPythonInterpreterException, TimeoutException
  {
    Map<Integer,PythonRequestResponse<T>> statusOfEval = new HashMap<>();
    if (!executionMode.equals(WorkerExecutionMode.ALL_WORKERS)) {
      InterpreterWrapper currentThread = selectWorkerForCurrentCall(requestId);
      if ( currentThread != null) {
        statusOfEval.put(currentThread.getInterpreterId(),
          currentThread.eval(windowId,requestId,command,variableNameToFetch,globalMethodsParams, timeout,timeUnit,
              deleteExtractedVariable,expectedReturnType));
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
        statusOfEval.put(wrapper.getInterpreterId(),
          wrapper.eval(windowId,requestId,command,variableNameToFetch,globalMethodsParams, timeOutPerWorker,timeUnit,
            deleteExtractedVariable,expectedReturnType));
      }
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
}
