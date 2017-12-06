package org.apache.apex.malhar.python.base.jep;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.PythonRequestResponse;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;

/**
 *
 */

public class InterpreterThread implements Runnable
{
  private static final Logger LOG = LoggerFactory.getLogger(InterpreterThread.class);

  public static final String JEP_LIBRARY_NAME = "jep";
  public static final String PYTHON_DEL_COMMAND = "del ";

  public static final String PYTHON_INCLUDE_PATHS = "PYTHON_INCLUDE_PATHS";

  public transient Jep JEP_INSTANCE;

  private transient volatile boolean isStopped = false;

  private transient boolean isBusy = false;

  private long timeOutToPollFromRequestQueue = 5;

  private TimeUnit timeUnitsToPollFromRequestQueue = TimeUnit.MILLISECONDS;

  private transient BlockingQueue<PythonRequestResponse> requestQueue;

  private transient BlockingQueue<PythonRequestResponse> responseQueue;

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
    LOG.info("Java library path being used is: " + System.getProperty("java.library.path"));
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
      return type.cast(JEP_INSTANCE.invoke(nameOfGlobalMethod,argsToGlobalMethod.toArray()));
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
    LOG.debug(" params for eval passed in are " + command + " " + expectedReturnType);
    try {
      for (String aKey : globalMethodsParams.keySet()) {
        if (globalMethodsParams.get(aKey) instanceof Integer) {
          JEP_INSTANCE.set(aKey, (int)globalMethodsParams.get(aKey));
        }
        if (globalMethodsParams.get(aKey) instanceof Long) {
          JEP_INSTANCE.set(aKey, (long)globalMethodsParams.get(aKey));
        }
      }
    } catch (JepException e) {
      LOG.debug("Error while setting the params for eval expression " + command, e);
      return null;
    }
    try {
      JEP_INSTANCE.eval(command);
    } catch (JepException e) {
      LOG.debug("Error while evaluating the expresions " + command, e);
      return null;
    }
    try {
      if (variableToExtract != null) {
        variableToReturn =  expectedReturnType.cast(JEP_INSTANCE.getValue(variableToExtract));
        if (deleteExtractedVariable) {
          JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + variableToExtract);
        }
      }
      for (String aKey: globalMethodsParams.keySet()) {
        LOG.debug("deleting " + aKey);
        JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + aKey);
      }
    } catch (JepException e) {
      LOG.error("Error while evaluating expression " + command, e);
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
    LOG.debug("Processing command ");
    PythonRequestResponse requestResponseHandle = requestQueue.poll(timeOutToPollFromRequestQueue,
        timeUnitsToPollFromRequestQueue);
    if (requestResponseHandle != null) {
      isBusy = true;
      PythonRequestResponse<T>.PythonInterpreterRequest<T> request =
          requestResponseHandle.getPythonInterpreterRequest();
      PythonRequestResponse<T>.PythonInterpreterResponse<T> response =
          requestResponseHandle.getPythonInterpreterResponse();
      Map<String,Boolean> commandStatus = new HashMap<>(1);
      switch (request.getCommandType()) {
        case EVAL_COMMAND:
          T responseVal = eval(request.getEvalCommand(), request.getVariableNameToExtractInEvalCall(),
              request.getParamsForEvalCommand(), request.isDeleteVariableAfterEvalCall(),
              request.getExpectedReturnType());
          response.setResponse(responseVal);
          if (responseVal != null) {
            commandStatus.put(request.getEvalCommand(),Boolean.TRUE);
          } else {
            commandStatus.put(request.getEvalCommand(),Boolean.FALSE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case SCRIPT_COMMAND:
          if (executeScript(request.getScriptName())) {
            commandStatus.put(request.getScriptName(),Boolean.TRUE);
          } else {
            commandStatus.put(request.getScriptName(),Boolean.FALSE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case METHOD_INVOCATION_COMMAND:
          response.setResponse(executeMethodCall(request.getNameOfMethodForMethodCallInvocation(),
              request.getArgsToMethodCallInvocation(), request.getExpectedReturnType()));
          if (response.getResponse() == null) {
            commandStatus.put(request.getNameOfMethodForMethodCallInvocation(), Boolean.FALSE);
          } else {
            commandStatus.put(request.getNameOfMethodForMethodCallInvocation(), Boolean.TRUE);
          }
          response.setCommandStatus(commandStatus);
          break;
        case GENERIC_COMMANDS:
          response.setCommandStatus(runCommands(request.getGenericCommands()));
          break;
        default:
          throw new ApexPythonInterpreterException(new Exception("Unspecified Interpreter command"));
      }
      requestResponseHandle.setRequestCompletionTime(System.currentTimeMillis());
      responseQueue.put(requestResponseHandle);
    }
    isBusy = false;
  }

  @Override
  public void run()
  {
    LOG.info("Starting the execution of processing " );
    if (JEP_INSTANCE == null) {
      try {
        startInterpreter();
      } catch (ApexPythonInterpreterException e) {
        throw new RuntimeException(e);
      }
    }
    while (!isStopped) {
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
    return isBusy;
  }

  public void setBusy(boolean busy)
  {
    isBusy = busy;
  }
}
