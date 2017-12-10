package org.apache.apex.malhar.python.base.requestresponse;

public class PythonRequestResponse<T>
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
}

