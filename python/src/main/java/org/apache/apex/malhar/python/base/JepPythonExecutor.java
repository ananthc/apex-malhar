package org.apache.apex.malhar.python.base;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import jep.Jep;
import jep.JepConfig;
import jep.JepException;

/**
 * <p>A python engine that executes python code by initializing the python interpreter in the
 * same JVM as that of Apex Operator. Note that the use of the engine is to execute python code
 * only and does not implement ( though possible ) the approach of invoking JVM methods from within the python code</p>
 *
 *
 * <p>
 *   <ol>Some callouts with this interpreter integration with Apex
 *   <li>If the Apex operator is using either the CONTAINER_LOCAL or THREAD_LOCAL and if two of the APEX Python
 *    operators are loading the same Cpython module which has a global static variable, then there is a
 *     possibility of the static variable not behaving the same way if it were to be implemented invoked
 *      in separate JVM operators. The user of the operator will have to workaround if such a use case is needed.</li>
 *    <li>Shared modules across each operator instances are possible.If there is such a situation, this
 *     implementation will automatically filter common modules between
 *     {@code {@link ApexPythonEngine#startInterpreter()} ()}} and {@link ApexPythonEngine#runCommands(List, long,
 *       TimeUnit)}</li>
 *   </ol>
 * </p>
 *
 */

public class JepPythonExecutor
{
  public static final String JEP_LIBRARY_NAME = "jep";
  public transient Jep JEP_INSTANCE;

  //ArrayBlockingQueue<T>

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
    loadMandatoryJVMLibraries();
  }


  public void startInterpreter() throws ApexPythonInterpreterException
  {
    JepConfig config = new JepConfig()
        .setRedirectOutputStreams(true)
        .setInteractive(false)
        .setClassLoader(Thread.currentThread().getContextClassLoader()
    );
    try {
      JEP_INSTANCE = new Jep(config);
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }


  public Map<String,Boolean> runCommands(List<String> commands)
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

  public <T> T executeMethodCall(String nameOfGlobalMethod, List<Object> argsToGlobalMethod,
      T type) throws ApexPythonInterpreterException
  {
    try {
      return (T)JEP_INSTANCE.invoke(nameOfGlobalMethod,argsToGlobalMethod.toArray());
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }

  public void executeScript(String scriptName, Map<String, Object> globalParams)
    throws ApexPythonInterpreterException
  {
    try {
      for (String aKey: globalParams.keySet()) {
        JEP_INSTANCE.set(aKey, globalParams.get(aKey));
      }
      JEP_INSTANCE.runScript(scriptName);
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }

  public <T> T eval(String command, String variableToExtract, Map<String, Object> globalMethodsParams,
      T expectedReturnType) throws ApexPythonInterpreterException
  {
    try {
      for (String aKey: globalMethodsParams.keySet()) {
        JEP_INSTANCE.set(aKey, globalMethodsParams.get(aKey));
      }
      JEP_INSTANCE.eval(command);
      if (variableToExtract != null) {
        return (T)JEP_INSTANCE.getValue(variableToExtract);
      }
      return null;
    } catch (JepException e) {
      throw new ApexPythonInterpreterException(e);
    }
  }

  public void stopInterpreter() throws ApexPythonInterpreterException
  {
    JEP_INSTANCE.close();
  }


}
