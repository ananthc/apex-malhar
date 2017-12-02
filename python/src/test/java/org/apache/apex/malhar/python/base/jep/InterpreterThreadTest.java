package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.test.JepPythonTestContext;

public class InterpreterThreadTest extends BaseJEPTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(InterpreterThreadTest.class);


  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testRunCommands() throws Exception
  {
    long currentTime = System.currentTimeMillis();
    List<String> commands = new ArrayList();
    commands.add("fileHandle  = open('/tmp/testRunCommands.txt', 'w')");
    commands.add("fileHandle.write('" + currentTime + "')");
    commands.add("fileHandle.close()");
    PythonRequestResponse<Void> runCommandsRequest = buildRequestResponseObjectForVoidPayload(
        PythonRequestResponse.PythonCommandType.GENERIC_COMMANDS);
    runCommandsRequest.getPythonInterpreterRequest().setGenericCommands(commands);
    pythonEngineThread.getRequestQueue().put(runCommandsRequest);
    Thread.sleep(2225000);
  }
}
