package org.apache.apex.malhar.python.base.requestresponse;

import java.util.Map;

public class PythonInterpreterResponse<T>
{

  Class<T> responseTypeClass;

  Map<String,Boolean> commandStatus;

  public PythonInterpreterResponse(Class<T> responseTypeClassHandle)
  {
    responseTypeClass = responseTypeClassHandle;
  }

  T response;

  public T getResponse()
  {
    return response;
  }

  public void setResponse(T response)
  {
    this.response = response;
  }

  public Map<String, Boolean> getCommandStatus()
  {
    return commandStatus;
  }

  public void setCommandStatus(Map<String, Boolean> commandStatus)
  {
    this.commandStatus = commandStatus;
  }
}
