package org.apache.apex.malhar.python.base.jep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
    File tempFile = File.createTempFile("apexpythonunittestruncommands-", ".txt");
    tempFile.deleteOnExit();
    String filePath = tempFile.getAbsolutePath();
    assertEquals(0L,tempFile.length());

    List<String> commands = new ArrayList();
    commands.add("fileHandle  = open('" + filePath + "', 'w')");
    commands.add("fileHandle.write('" + currentTime + "')");
    commands.add("fileHandle.flush()");
    commands.add("fileHandle.close()");
    runCommands(commands);
    assertEquals(("" + currentTime).length(), tempFile.length());

    List<String> errorCommands = new ArrayList();
    errorCommands.add("1+2");
    errorCommands.add("3+");
    PythonRequestResponse<Void> response = runCommands(errorCommands);
    Map<String,Boolean> responseStatus = response.getPythonInterpreterResponse().getCommandStatus();
    assertTrue(responseStatus.get(errorCommands.get(0)));
    assertFalse(responseStatus.get(errorCommands.get(1)));
  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testMethodCall() throws Exception
  {
    String methodName = "jepMultiply";
    List<String> commands = new ArrayList();
    commands.add("def " + methodName + "(firstnum, secondnum):\n" +
        "\treturn (firstnum * secondnum)\n"); // Note that this cannot be split as multiple commands
    runCommands(commands);

    List<Long> params = new ArrayList<>();
    params.add(5L);
    params.add(25L);

    PythonRequestResponse<Long> methodCallRequest = buildRequestResponseObjectForLongPayload(
        PythonRequestResponse.PythonCommandType.METHOD_INVOCATION_COMMAND);
    methodCallRequest.getPythonInterpreterRequest().setNameOfMethodForMethodCallInvocation(methodName);
    methodCallRequest.getPythonInterpreterRequest().setArgsToMethodCallInvocation(params);
    methodCallRequest.getPythonInterpreterRequest().setExpectedReturnType(Long.class);

    pythonEngineThread.getRequestQueue().put(methodCallRequest);
    Thread.sleep(1000); // wait for command to be processed
    PythonRequestResponse<Long> methodCallResponse = pythonEngineThread.getResponseQueue().poll(1,
        TimeUnit.SECONDS);
    assertEquals(methodCallResponse.getPythonInterpreterResponse().getResponse(),125L);
    Map<String,Boolean> commandStatus = methodCallResponse.getPythonInterpreterResponse().getCommandStatus();
    assertTrue(commandStatus.get(methodName));

    params.remove(1);
    methodCallRequest = buildRequestResponseObjectForLongPayload(
      PythonRequestResponse.PythonCommandType.METHOD_INVOCATION_COMMAND);
    methodCallRequest.getPythonInterpreterRequest().setNameOfMethodForMethodCallInvocation(methodName);
    methodCallRequest.getPythonInterpreterRequest().setArgsToMethodCallInvocation(params);
    methodCallRequest.getPythonInterpreterRequest().setExpectedReturnType(Long.class);

    pythonEngineThread.getRequestQueue().put(methodCallRequest);
    Thread.sleep(1000); // wait for command to be processed
    methodCallResponse = pythonEngineThread.getResponseQueue().poll(1, TimeUnit.SECONDS);
    commandStatus = methodCallResponse.getPythonInterpreterResponse().getCommandStatus();
    assertFalse(commandStatus.get(methodName));
  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testScriptCall() throws Exception
  {
    File tempFileForScript = File.createTempFile("apexpythonunittestscript-", ".py");
    tempFileForScript.deleteOnExit();
    String filePathForFactorialScript = tempFileForScript.getAbsolutePath();
    migrateFileFromResourcesFolderToTemp("factorial.py",filePathForFactorialScript);
    PythonRequestResponse<Void> methodCallRequest = buildRequestResponseObjectForVoidPayload(
        PythonRequestResponse.PythonCommandType.SCRIPT_COMMAND);
    methodCallRequest.getPythonInterpreterRequest().setScriptName(filePathForFactorialScript);
    pythonEngineThread.getRequestQueue().put(methodCallRequest);
    Thread.sleep(1000); // wait for command to be processed
    PythonRequestResponse<Void> methodCallResponse = pythonEngineThread.getResponseQueue().poll(1,
        TimeUnit.SECONDS);
    Map<String,Boolean> commandStatus = methodCallResponse.getPythonInterpreterResponse().getCommandStatus();
    assertTrue(commandStatus.get(filePathForFactorialScript));
    try (BufferedReader br = new BufferedReader(new FileReader("target/factorial-result.txt"))) {
      String line;
      while ((line = br.readLine()) != null) {
        assertEquals(120,Integer.parseInt(line)); // asset factorial is calculated as written in script in resources
        break; // There is only one line in the file per the python script
      }
    }
  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testEvalCall() throws Exception
  {
    String expression = new String("x = a + b");
    Random random = new Random();
    int a = random.nextInt(100);
    int b = random.nextInt(100);
    Map<String,Object> argsForEval = new HashMap<>();
    argsForEval.put("a",a);
    argsForEval.put("b",b);
    PythonRequestResponse<Long> methodCallRequest = buildRequestResponseObjectForLongPayload(
        PythonRequestResponse.PythonCommandType.EVAL_COMMAND);
    methodCallRequest.getPythonInterpreterRequest().setEvalCommand(expression);
    methodCallRequest.getPythonInterpreterRequest().setParamsForEvalCommand(argsForEval);
    methodCallRequest.getPythonInterpreterRequest().setDeleteVariableAfterEvalCall(true);
    methodCallRequest.getPythonInterpreterRequest().setVariableNameToExtractInEvalCall("x");
    methodCallRequest.getPythonInterpreterRequest().setExpectedReturnType(Long.class);
    pythonEngineThread.getRequestQueue().put(methodCallRequest);
    Thread.sleep(1000); // wait for command to be processed
    PythonRequestResponse<Integer> methodCallResponse = pythonEngineThread.getResponseQueue().poll(1,
        TimeUnit.SECONDS);
    Map<String,Boolean> commandStatus = methodCallResponse.getPythonInterpreterResponse().getCommandStatus();
    assertTrue(commandStatus.get(expression));
    assertEquals(methodCallResponse.getPythonInterpreterResponse().getResponse(),(long)(a + b));
  }

}
