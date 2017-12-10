package org.apache.apex.malhar.python.base.requestresponse;

import java.util.List;

public class GenericCommandsRequestPayload
{
  List<String> genericCommands;

  public List<String> getGenericCommands()
  {
    return genericCommands;
  }

  public void setGenericCommands(List<String> genericCommands)
  {
    this.genericCommands = genericCommands;
  }
}
