package org.apache.apex.malhar.python.base.jep;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

  static ExecutorService executorService = Executors.newSingleThreadExecutor();

  static BlockingQueue<PythonRequestResponse> requestQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8, SpinPolicy.WAITING);

  static BlockingQueue<PythonRequestResponse> responseQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8,SpinPolicy.WAITING);


  public static void initJEPThread() throws Exception
  {
    if (!JEP_INITIALIZED) {
      synchronized (lockToInitializeJEP) {
        if (!JEP_INITIALIZED) {
          pythonEngineThread = new InterpreterThread(requestQueue,responseQueue);
          pythonEngineThread.preInitInterpreter(new HashMap<String,Object>());
          pythonEngineThread.startInterpreter();
          executorService.submit(pythonEngineThread);
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
}
