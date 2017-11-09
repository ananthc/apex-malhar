package org.apache.apex.malhar.python.base;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PythonRequestResponse
{
  PythonInterpreterRequest pythonInterpreterRequest;

  PythonInterpreterResponse pythonInterpreterResponse;

  long requestId;

  long windowId;

  long requestStartTime;

  long requestCompletionTime;

  public PythonInterpreterRequest getPythonInterpreterRequest()
  {
    return pythonInterpreterRequest;
  }

  public void setPythonInterpreterRequest(PythonInterpreterRequest pythonInterpreterRequest)
  {
    this.pythonInterpreterRequest = pythonInterpreterRequest;
  }

  public PythonInterpreterResponse getPythonInterpreterResponse()
  {
    return pythonInterpreterResponse;
  }

  public void setPythonInterpreterResponse(PythonInterpreterResponse pythonInterpreterResponse)
  {
    this.pythonInterpreterResponse = pythonInterpreterResponse;
  }

  public long getRequestId()
  {
    return requestId;
  }

  public void setRequestId(long requestId)
  {
    this.requestId = requestId;
  }

  public long getWindowId()
  {
    return windowId;
  }

  public void setWindowId(long windowId)
  {
    this.windowId = windowId;
  }

  public long getRequestStartTime()
  {
    return requestStartTime;
  }

  public void setRequestStartTime(long requestStartTime)
  {
    this.requestStartTime = requestStartTime;
  }

  public long getRequestCompletionTime()
  {
    return requestCompletionTime;
  }

  public void setRequestCompletionTime(long requestCompletionTime)
  {
    this.requestCompletionTime = requestCompletionTime;
  }



  public class PythonInterpreterRequest<T>
  {
    PythonCommandType commandType;



    List<String> genericCommands;

    long timeOut;

    TimeUnit timeUnit;

    String nameOfMethodForMethodCallInvocation;

    List<Object> argsToMethodCallInvocation;

    String scriptName;

    Map<String, Object> methodParamsForScript;

    boolean deleteVariableAfterEvalCall;

    String variableNameToExtractInEvalCall;

    String evalCommand;

    Map<String,Object> paramsForEvalCommand;

    Class<T> expectedReturnType;

    public PythonCommandType getCommandType()
    {
      return commandType;
    }

    public void setCommandType(PythonCommandType commandType)
    {
      this.commandType = commandType;
    }

    public long getTimeOut()
    {
      return timeOut;
    }

    public void setTimeOut(long timeOut)
    {
      this.timeOut = timeOut;
    }

    public TimeUnit getTimeUnit()
    {
      return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit)
    {
      this.timeUnit = timeUnit;
    }

    public String getNameOfMethodForMethodCallInvocation()
    {
      return nameOfMethodForMethodCallInvocation;
    }

    public void setNameOfMethodForMethodCallInvocation(String nameOfMethodForMethodCallInvocation)
    {
      this.nameOfMethodForMethodCallInvocation = nameOfMethodForMethodCallInvocation;
    }

    public List<Object> getArgsToMethodCallInvocation()
    {
      return argsToMethodCallInvocation;
    }

    public void setArgsToMethodCallInvocation(List<Object> argsToMethodCallInvocation)
    {
      this.argsToMethodCallInvocation = argsToMethodCallInvocation;
    }

    public String getScriptName()
    {
      return scriptName;
    }

    public void setScriptName(String scriptName)
    {
      this.scriptName = scriptName;
    }

    public Map<String, Object> getMethodParamsForScript()
    {
      return methodParamsForScript;
    }

    public void setMethodParamsForScript(Map<String, Object> methodParamsForScript)
    {
      this.methodParamsForScript = methodParamsForScript;
    }

    public boolean isDeleteVariableAfterEvalCall()
    {
      return deleteVariableAfterEvalCall;
    }

    public void setDeleteVariableAfterEvalCall(boolean deleteVariableAfterEvalCall)
    {
      this.deleteVariableAfterEvalCall = deleteVariableAfterEvalCall;
    }

    public String getVariableNameToExtractInEvalCall()
    {
      return variableNameToExtractInEvalCall;
    }

    public void setVariableNameToExtractInEvalCall(String variableNameToExtractInEvalCall)
    {
      this.variableNameToExtractInEvalCall = variableNameToExtractInEvalCall;
    }

    public String getEvalCommand()
    {
      return evalCommand;
    }

    public void setEvalCommand(String evalCommand)
    {
      this.evalCommand = evalCommand;
    }

    public List<String> getGenericCommands()
    {
      return genericCommands;
    }

    public void setGenericCommands(List<String> genericCommands)
    {
      this.genericCommands = genericCommands;
    }

    public Map<String, Object> getParamsForEvalCommand()
    {
      return paramsForEvalCommand;
    }

    public void setParamsForEvalCommand(Map<String, Object> paramsForEvalCommand)
    {
      this.paramsForEvalCommand = paramsForEvalCommand;
    }

    public Class<T> getExpectedReturnType()
    {
      return expectedReturnType;
    }

    public void setExpectedReturnType(Class<T> expectedReturnType)
    {
      this.expectedReturnType = expectedReturnType;
    }
  }

  public class PythonInterpreterResponse<T>
  {

    T response;

    public T getResponse()
    {
      return response;
    }

    public void setResponse(T response)
    {
      this.response = response;
    }
  }

  public enum PythonCommandType
  {
    GENERIC_COMMANDS,
    EVAL_COMMAND,
    SCRIPT_COMMAND,
    METHOD_INVOCATION_COMMAND
  }
}
