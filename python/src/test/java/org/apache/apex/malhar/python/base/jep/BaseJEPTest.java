package org.apache.apex.malhar.python.base.jep;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.PythonRequestResponse;
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

          // interpreter for wrapper based tests
          interpreterWrapper = new InterpreterWrapper("unit-test-wrapper",delayedResponseQueueForWrapper);

          JEP_INITIALIZED = true;
        }
      }
    }
  }

  public PythonRequestResponse<Void> buildRequestResponseObjectForVoidPayload(
      PythonRequestResponse.PythonCommandType commandType) throws Exception
  {
    PythonRequestResponse<Void> requestResponse = new PythonRequestResponse();
    PythonRequestResponse<Void>.PythonInterpreterRequest<Void> request =
        requestResponse.new PythonInterpreterRequest<>();
    PythonRequestResponse<Void>.PythonInterpreterResponse<Void> response =
        requestResponse.new PythonInterpreterResponse<>(Void.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    request.setCommandType(commandType);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(1L);
    requestResponse.setWindowId(1L);
    return requestResponse;
  }

  public PythonRequestResponse<Long> buildRequestResponseObjectForLongPayload(
      PythonRequestResponse.PythonCommandType commandType) throws Exception
  {
    PythonRequestResponse<Long> requestResponse = new PythonRequestResponse();
    PythonRequestResponse<Long>.PythonInterpreterRequest<Long> request =
        requestResponse.new PythonInterpreterRequest<>();
    PythonRequestResponse<Long>.PythonInterpreterResponse<Long> response =
        requestResponse.new PythonInterpreterResponse<>(Long.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    request.setCommandType(commandType);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(1L);
    requestResponse.setWindowId(1L);
    return requestResponse;
  }



  public PythonRequestResponse<Integer> buildRequestResponseObjectForIntPayload(
      PythonRequestResponse.PythonCommandType commandType) throws Exception
  {
    PythonRequestResponse<Integer> requestResponse = new PythonRequestResponse();
    PythonRequestResponse<Integer>.PythonInterpreterRequest<Integer> request =
        requestResponse.new PythonInterpreterRequest<>();
    PythonRequestResponse<Integer>.PythonInterpreterResponse<Integer> response =
        requestResponse.new PythonInterpreterResponse<>(Integer.class);
    requestResponse.setPythonInterpreterRequest(request);
    requestResponse.setPythonInterpreterResponse(response);
    request.setCommandType(commandType);
    requestResponse.setRequestStartTime(System.currentTimeMillis());
    requestResponse.setRequestId(1L);
    requestResponse.setWindowId(1L);
    return requestResponse;
  }


  protected PythonRequestResponse<Void> runCommands(List<String> commands) throws Exception
  {
    PythonRequestResponse<Void> runCommandsRequest = buildRequestResponseObjectForVoidPayload(
      PythonRequestResponse.PythonCommandType.GENERIC_COMMANDS);
    runCommandsRequest.getPythonInterpreterRequest().setGenericCommands(commands);
    pythonEngineThread.getRequestQueue().put(runCommandsRequest);
    Thread.sleep(1000); // wait for command to be processed
    return pythonEngineThread.getResponseQueue().poll(1, TimeUnit.SECONDS);
  }

}
