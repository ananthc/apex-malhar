package org.apache.apex.malhar.python.base.requestresponse;

import java.util.List;

public class MethodCallRequestPayload
{
  private String nameOfMethod;

  private List<Object> args;

  public String getNameOfMethod()
  {
    return nameOfMethod;
  }

  public void setNameOfMethod(String nameOfMethod)
  {
    this.nameOfMethod = nameOfMethod;
  }

  public List<Object> getArgs()
  {
    return args;
  }

  public void setArgs(List<Object> args)
  {
    this.args = args;
  }
}
