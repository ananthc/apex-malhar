package org.apache.apex.malhar.python.base.requestresponse;

public class PythonInterpreterRequest<T>
{
  PythonCommandType commandType;

  long timeOutInNanos;

  MethodCallRequestPayload methodCallRequest;

  GenericCommandsRequestPayload genericCommandsRequestPayload;

  EvalCommandRequestPayload evalCommandRequestPayload;

  ScriptExecutionRequestPayload scriptExecutionRequestPayload;

  Class<T> expectedReturnType;

  public PythonCommandType getCommandType()
  {
    return commandType;
  }

  public void setCommandType(PythonCommandType commandType)
  {
    this.commandType = commandType;
  }

  public long getTimeOutInNanos()
  {
    return timeOutInNanos;
  }

  public void setTimeOutInNanos(long timeOutInNanos)
  {
    this.timeOutInNanos = timeOutInNanos;
  }

  public MethodCallRequestPayload getMethodCallRequest()
  {
    return methodCallRequest;
  }

  public void setMethodCallRequest(MethodCallRequestPayload methodCallRequest)
  {
    this.methodCallRequest = methodCallRequest;
  }

  public GenericCommandsRequestPayload getGenericCommandsRequestPayload()
  {
    return genericCommandsRequestPayload;
  }

  public void setGenericCommandsRequestPayload(GenericCommandsRequestPayload genericCommandsRequestPayload)
  {
    this.genericCommandsRequestPayload = genericCommandsRequestPayload;
  }

  public EvalCommandRequestPayload getEvalCommandRequestPayload()
  {
    return evalCommandRequestPayload;
  }

  public void setEvalCommandRequestPayload(EvalCommandRequestPayload evalCommandRequestPayload)
  {
    this.evalCommandRequestPayload = evalCommandRequestPayload;
  }

  public ScriptExecutionRequestPayload getScriptExecutionRequestPayload()
  {
    return scriptExecutionRequestPayload;
  }

  public void setScriptExecutionRequestPayload(ScriptExecutionRequestPayload scriptExecutionRequestPayload)
  {
    this.scriptExecutionRequestPayload = scriptExecutionRequestPayload;
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
