package org.apache.apex.malhar.python.base.jep;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.test.JepPythonTestContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InterpreterThreadTest extends BaseJEPTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(InterpreterThreadTest.class);


  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testRunCommands() throws Exception
  {
    long currentTime = System.currentTimeMillis();
    File tempFile = File.createTempFile("apexpythonunittestruncommands", "txt");
    String filePath = tempFile.getAbsolutePath();
    assertEquals(0L,tempFile.length());
    List<String> commands = new ArrayList();
    commands.add("fileHandle  = open('" + filePath + "', 'w')");
    commands.add("fileHandle.write('" + currentTime + "')");
    commands.add("fileHandle.close()");
    PythonRequestResponse<Void> runCommandsRequest = buildRequestResponseObjectForVoidPayload(
        PythonRequestResponse.PythonCommandType.GENERIC_COMMANDS);
    runCommandsRequest.getPythonInterpreterRequest().setGenericCommands(commands);
    pythonEngineThread.getRequestQueue().put(runCommandsRequest);
    Thread.sleep(1000); // wait for command to be processed
    pythonEngineThread.getResponseQueue().poll(1, TimeUnit.SECONDS); // drain response queue before next request
    assertEquals(("" + currentTime).length(), tempFile.length());
    List<String> errorCommands = new ArrayList();
    errorCommands.add("1+2");
    errorCommands.add("3+");
    runCommandsRequest = buildRequestResponseObjectForVoidPayload(
      PythonRequestResponse.PythonCommandType.GENERIC_COMMANDS);
    runCommandsRequest.getPythonInterpreterRequest().setGenericCommands(errorCommands);
    pythonEngineThread.getRequestQueue().put(runCommandsRequest);
    Thread.sleep(500); // wait for command to be processed
    PythonRequestResponse<Void> response = pythonEngineThread.getResponseQueue().poll(1, TimeUnit.SECONDS);
    Map<String,Boolean> responseStatus = response.getPythonInterpreterResponse().getCommandStatus();
    assertTrue(responseStatus.get(errorCommands.get(0)));
    assertFalse(responseStatus.get(errorCommands.get(1)));
    tempFile.deleteOnExit();
  }
}
