package org.apache.apex.malhar.python.base.jep;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.ApexPythonInterpreterException;
import org.apache.apex.malhar.python.base.requestresponse.EvalCommandRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.GenericCommandsRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.MethodCallRequestPayload;
import org.apache.apex.malhar.python.base.requestresponse.PythonCommandType;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterResponse;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
import org.apache.apex.malhar.python.base.requestresponse.ScriptExecutionRequestPayload;
import org.apache.apex.malhar.python.test.BasePythonTest;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;

public class BaseJEPTest extends BasePythonTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(BaseJEPTest.class);

  public static boolean JEP_INITIALIZED = false;

  private static  Object lockToInitializeJEP = new Object();

  static InterpreterThread pythonEngineThread;

  static InterpreterWrapper interpreterWrapper;

  static JepPythonEngine jepPythonEngine;

  static ExecutorService executorService = Executors.newSingleThreadExecutor();

  static BlockingQueue<PythonRequestResponse> requestQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8, SpinPolicy.WAITING);

  static BlockingQueue<PythonRequestResponse> responseQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8,SpinPolicy.WAITING);

  static BlockingQueue<PythonRequestResponse> delayedResponseQueueForWrapper =
      new DisruptorBlockingQueue<PythonRequestResponse>(8, SpinPolicy.WAITING);


  public static void initJEPThread() throws Exception
  {
    if (!JEP_INITIALIZED) {
      synchronized (lockToInitializeJEP) {
        if (!JEP_INITIALIZED) {
          // Interpreter for thread based tests
          pythonEngineThread = new InterpreterThread(requestQueue,responseQueue,"unittests-1");
          pythonEngineThread.preInitInterpreter(new HashMap<String,Object>());
          executorService.submit(pythonEngineThread);

          // interpreter wrapper for wrapper based tests
          interpreterWrapper = new InterpreterWrapper("unit-test-wrapper",delayedResponseQueueForWrapper);
          interpreterWrapper.startInterpreter();

          // JEP python engine tests
          jepPythonEngine = new JepPythonEngine("unit-tests-jeppythonengine",5);
          jepPythonEngine.preInitInterpreter(new HashMap<String,Object>());
          jepPythonEngine.startInterpreter();
          JEP_INITIALIZED = true;
        }
      }
    }
  }

  private void setCommonConstructsForRequestResponseObject(PythonCommandType commandType,
      PythonInterpreterRequest request, PythonRequestResponse requestResponse )
    throws ApexPythonInterpreterException
  {
    request.setCommandType(commandType);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(1L);
    requestResponse.setWindowId(1L);
    switch (commandType) {
      case EVAL_COMMAND:
        EvalCommandRequestPayload payload = new EvalCommandRequestPayload();
        request.setEvalCommandRequestPayload(payload);
        break;
      case METHOD_INVOCATION_COMMAND:
        MethodCallRequestPayload methodCallRequest = new MethodCallRequestPayload();
        request.setMethodCallRequest(methodCallRequest);
        break;
      case SCRIPT_COMMAND:
        ScriptExecutionRequestPayload scriptPayload = new ScriptExecutionRequestPayload();
        request.setScriptExecutionRequestPayload(scriptPayload);
        break;
      case GENERIC_COMMANDS:
        GenericCommandsRequestPayload payloadForGenericCommands = new GenericCommandsRequestPayload();
        request.setGenericCommandsRequestPayload(payloadForGenericCommands);
        break;
      default:
        throw new ApexPythonInterpreterException("Unsupported command type");
    }

  }

  public PythonRequestResponse<Void> buildRequestResponseObjectForVoidPayload(PythonCommandType commandType)
      throws Exception
  {
    PythonRequestResponse<Void> requestResponse = new PythonRequestResponse();
    PythonInterpreterRequest<Void> request = new PythonInterpreterRequest<>();
    PythonInterpreterResponse<Void> response = new PythonInterpreterResponse<>(Void.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    setCommonConstructsForRequestResponseObject(commandType,request,requestResponse);
    return requestResponse;
  }

  public PythonRequestResponse<Long> buildRequestResponseObjectForLongPayload(
      PythonCommandType commandType) throws Exception
  {
    PythonRequestResponse<Long> requestResponse = new PythonRequestResponse();
    PythonInterpreterRequest<Long> request = new PythonInterpreterRequest<>();
    requestResponse.setPythonInterpreterRequest(request);
    PythonInterpreterResponse<Long> response = new PythonInterpreterResponse<>(Long.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    setCommonConstructsForRequestResponseObject(commandType,request,requestResponse);
    return requestResponse;
  }



  public PythonRequestResponse<Integer> buildRequestResponseObjectForIntPayload(
      PythonCommandType commandType) throws Exception
  {
    PythonRequestResponse<Integer> requestResponse = new PythonRequestResponse();
    PythonInterpreterRequest<Integer> request = new PythonInterpreterRequest<>();
    requestResponse.setPythonInterpreterRequest(request);
    PythonInterpreterResponse<Integer> response = new PythonInterpreterResponse<>(Integer.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    setCommonConstructsForRequestResponseObject(commandType,request,requestResponse);
    return requestResponse;
  }


  protected PythonRequestResponse<Void> runCommands(List<String> commands) throws Exception
  {
    PythonRequestResponse<Void> runCommandsRequest = buildRequestResponseObjectForVoidPayload(
        PythonCommandType.GENERIC_COMMANDS);
    runCommandsRequest.getPythonInterpreterRequest().getGenericCommandsRequestPayload().setGenericCommands(commands);
    pythonEngineThread.getRequestQueue().put(runCommandsRequest);
    Thread.sleep(1000); // wait for command to be processed
    return pythonEngineThread.getResponseQueue().poll(1, TimeUnit.SECONDS);
  }

}
