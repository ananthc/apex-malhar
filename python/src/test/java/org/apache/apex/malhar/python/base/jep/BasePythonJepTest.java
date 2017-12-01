package org.apache.apex.malhar.python.base.jep;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.test.JepPythonTestContext;
import org.apache.apex.malhar.python.test.PythonAvailabilityTestRule;

import com.conversantmedia.util.concurrent.DisruptorBlockingQueue;
import com.conversantmedia.util.concurrent.SpinPolicy;


public class BasePythonJepTest
{

  @Rule
  public PythonAvailabilityTestRule jepAvailabilityBasedTest = new PythonAvailabilityTestRule();

  private static final transient Logger LOG = LoggerFactory.getLogger(BasePythonJepTest.class);

  static InterpreterThread pythonEngineThread;

  static ExecutorService executorService = Executors.newSingleThreadExecutor();

  static BlockingQueue<PythonRequestResponse> requestQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8,SpinPolicy.WAITING);

  static BlockingQueue<PythonRequestResponse> responseQueue =
      new DisruptorBlockingQueue<PythonRequestResponse>(8,SpinPolicy.WAITING);


  public static void testThreadInit() throws Exception
  {
    pythonEngineThread = new InterpreterThread(requestQueue,responseQueue);
    pythonEngineThread.preInitInterpreter(new HashMap<String,Object>());
    pythonEngineThread.startInterpreter();
    executorService.submit(pythonEngineThread);
  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testImportCommand() throws Exception
  {
    LOG.info("Loaded the class..");
  }

}
