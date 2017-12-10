package org.apache.apex.malhar.python.base.requestresponse;

import java.util.Map;

public class EvalCommandRequestPayload
{
  private boolean deleteVariableAfterEvalCall;

  private String variableNameToExtractInEvalCall;

  private String evalCommand;

  private Map<String,Object> paramsForEvalCommand;

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

  public Map<String, Object> getParamsForEvalCommand()
  {
    return paramsForEvalCommand;
  }

  public void setParamsForEvalCommand(Map<String, Object> paramsForEvalCommand)
  {
    this.paramsForEvalCommand = paramsForEvalCommand;
  }
}
