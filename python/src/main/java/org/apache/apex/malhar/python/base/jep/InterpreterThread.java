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

  private transient boolean isAlive = true;

  private long timeOutToPollFromRequestQueue = 5;

  private TimeUnit timeUnitsToPollFromRequestQueue = TimeUnit.MILLISECONDS;

  private transient BlockingQueue<PythonRequestResponse> requestQueue;

  private transient BlockingQueue<PythonRequestResponse> responseQueue;

  private Map<String,Object> initConfigs = new HashMap<>();

  public InterpreterThread(BlockingQueue<PythonRequestResponse> requestQueue,
      BlockingQueue<PythonRequestResponse> responseQueue)
  {
    this.requestQueue = requestQueue;
    this.responseQueue = responseQueue;
  }

  private void loadMandatoryJVMLibraries() throws ApexPythonInterpreterException
  {
    System.loadLibrary(JEP_LIBRARY_NAME);
  }


  public Jep getEngineReference() throws ApexPythonInterpreterException
  {
    return JEP_INSTANCE;
  }

  public void preInitInterpreter(Map<String, Object> preInitConfigs) throws ApexPythonInterpreterException
  {
    initConfigs.putAll(preInitConfigs);
    loadMandatoryJVMLibraries();
  }


  public void startInterpreter() throws ApexPythonInterpreterException
  {

    JepConfig config = new JepConfig()
        .setRedirectOutputStreams(true)
        .setInteractive(false)
        .setClassLoader(Thread.currentThread().getContextClassLoader()
    );
    if (initConfigs.containsKey(PYTHON_INCLUDE_PATHS)){
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


  private Map<String,Boolean> runCommands(List<String> commands)
      throws ApexPythonInterpreterException
  {
    Map<String,Boolean> resultsOfExecution = new HashMap<>();
    for (String aCommand : commands) {
      try {
        resultsOfExecution.put(aCommand,JEP_INSTANCE.eval(aCommand));
      } catch (JepException e) {
        resultsOfExecution.put(aCommand,Boolean.FALSE);
        throw new ApexPythonInterpreterException(e);
      }
    }
    return resultsOfExecution;
  }

  private <T> T executeMethodCall(String nameOfGlobalMethod, List<Object> argsToGlobalMethod,
      Class<T> type) throws ApexPythonInterpreterException
  {
    try {
      return (T)JEP_INSTANCE.invoke(nameOfGlobalMethod,argsToGlobalMethod.toArray());
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }

  private void executeScript(String scriptName, Map<String, Object> globalParams)
    throws ApexPythonInterpreterException
  {
    try {
      for (String aKey: globalParams.keySet()) {
        JEP_INSTANCE.set(aKey, globalParams.get(aKey));
      }
      JEP_INSTANCE.runScript(scriptName);
      for (String aKey: globalParams.keySet()) {
        JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + aKey);
      }
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }

  private <T> T eval(String command, String variableToExtract, Map<String, Object> globalMethodsParams,
    boolean deleteExtractedVariable,Class<T> expectedReturnType) throws ApexPythonInterpreterException
  {
    T variableToReturn = null;
    try {
      for (String aKey: globalMethodsParams.keySet()) {
        JEP_INSTANCE.set(aKey, globalMethodsParams.get(aKey));
      }
      JEP_INSTANCE.eval(command);
      for (String aKey: globalMethodsParams.keySet()) {
        JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + aKey);
      }
      if (variableToExtract != null) {
        variableToReturn =  (T)JEP_INSTANCE.getValue(variableToExtract);
        if (deleteExtractedVariable) {
          JEP_INSTANCE.eval(PYTHON_DEL_COMMAND + variableToExtract);
        }
      }
      return variableToReturn;
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }

  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    isAlive = false;
    JEP_INSTANCE.close();
  }

  private <T> void processCommand() throws ApexPythonInterpreterException, InterruptedException
  {
    PythonRequestResponse requestResponseHandle = requestQueue.poll(timeOutToPollFromRequestQueue,
      timeUnitsToPollFromRequestQueue);
    if (requestResponseHandle != null) {
      PythonRequestResponse<T>.PythonInterpreterRequest<T> request = requestResponseHandle.getPythonInterpreterRequest();
      PythonRequestResponse<T>.PythonInterpreterResponse<T> response =
        requestResponseHandle.getPythonInterpreterResponse();
      Map<String,Boolean> commandStatus = new HashMap<>(1);
      switch (request.getCommandType()) {
        case EVAL_COMMAND:
          response.setResponse(eval(request.getEvalCommand(), request.getVariableNameToExtractInEvalCall(),
            request.getParamsForEvalCommand(), request.isDeleteVariableAfterEvalCall(),
            request.getExpectedReturnType()));
          commandStatus.put(request.getEvalCommand(),Boolean.TRUE);
          response.setCommandStatus(commandStatus);
          break;
        case SCRIPT_COMMAND:
          executeScript(request.getScriptName(), request.getMethodParamsForScript());
          commandStatus.put(request.getScriptName(),Boolean.TRUE);
          break;
        case METHOD_INVOCATION_COMMAND:
          response.setResponse(executeMethodCall(request.getNameOfMethodForMethodCallInvocation(),
            request.getArgsToMethodCallInvocation(), request.getExpectedReturnType()));
          commandStatus.put(request.getNameOfMethodForMethodCallInvocation(),Boolean.TRUE);
          response.setCommandStatus(commandStatus);
          break;
        case GENERIC_COMMANDS:
          response.setCommandStatus(runCommands(request.getGenericCommands()));
          break;
      }
    }
    requestResponseHandle.setRequestCompletionTime(System.currentTimeMillis());
    responseQueue.put(requestResponseHandle);
  }

  @Override
  public void run()
  {
    while (isAlive) {
      try {
          processCommand();
        }
      catch (InterruptedException| ApexPythonInterpreterException e) {
        throw new RuntimeException(e);
      }
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

  public boolean isAlive()
  {
    return isAlive;
  }

  public void setAlive(boolean alive)
  {
    isAlive = alive;
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
}
