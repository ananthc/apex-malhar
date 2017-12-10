package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.base.WorkerExecutionMode;
import org.apache.apex.malhar.python.test.JepPythonTestContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class JepPythonEngineTest extends BaseJEPTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(JepPythonEngineTest.class);

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testSelectWorkerForCurrentCall() throws Exception
  {
    Set<String> overloadedWorkers = new HashSet<>();
    List<String> busyCommands = new ArrayList<>();
    busyCommands.add("import time");
    busyCommands.add("time.sleep(5)");
    for ( int i = 0; i < jepPythonEngine.getNumWorkerThreads() - 1; i++) {
      InterpreterWrapper aWrapper = jepPythonEngine.getWorkers().get(i);
      aWrapper.runCommands(1L,i,busyCommands,2, TimeUnit.MILLISECONDS);
      assertTrue(aWrapper.isCurrentlyBusy());
      overloadedWorkers.add(aWrapper.getInterpreterId());
    }
    InterpreterWrapper candidateWrapperForExecution = null;
    InterpreterWrapper validCandidateWrapperForExecution = null;
    int counterForNullWorkers = 0;
    int counterForValidWorkers = 0;
    for ( int i = 0; i < jepPythonEngine.getNumWorkerThreads(); i++) {
      candidateWrapperForExecution = jepPythonEngine.selectWorkerForCurrentCall(i);
      if ( candidateWrapperForExecution == null) {
        counterForNullWorkers += 1;
      } else {
        counterForValidWorkers += 1;
        validCandidateWrapperForExecution = candidateWrapperForExecution;
      }
    }
    // numWorker threads  because the select worker calls iterates over all workers to
    // get any of the free workers. We did not give any worker for the 5th worker and hence should pass for all calls
    assertEquals(jepPythonEngine.getNumWorkerThreads(), counterForValidWorkers);
    assertEquals( 0, counterForNullWorkers); // None of the attempts should fail to get a worker
    // Also we can only get that worker which has not been assigned a sleep instruction
    assertFalse(overloadedWorkers.contains(validCandidateWrapperForExecution.getInterpreterId()));
    Thread.sleep(5000); // all the python workers must be free after this line
    // we now test for all workers being busy
    for ( int i = 0; i < jepPythonEngine.getNumWorkerThreads(); i++) {
      InterpreterWrapper aWrapper = jepPythonEngine.getWorkers().get(i);
      aWrapper.runCommands(1L,i,busyCommands,2, TimeUnit.MILLISECONDS);
    }
    for (int i = 0; i < jepPythonEngine.getNumWorkerThreads(); i++) {
      candidateWrapperForExecution = jepPythonEngine.selectWorkerForCurrentCall(i);
      assertNull(candidateWrapperForExecution);
    }
    Thread.sleep(5000); // ensures other tests in this class can use this engine after this test
  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testWorkerExecutionMode() throws Exception
  {
    String methodName = "multiply";
    List<String> commands = new ArrayList();
    commands.add("def " + methodName + "(firstnum, secondnum):\n" +
        "\treturn (firstnum * secondnum)\n"); // Note that this cannot be split as multiple commands
    for (int i = 0; i < jepPythonEngine.getNumWorkerThreads(); i++) {
      InterpreterWrapper aWrapper = jepPythonEngine.getWorkers().get(i);
      aWrapper.runCommands(1L,1L,commands,1000,TimeUnit.MILLISECONDS);
    }
    HashMap<String,Object> params = new HashMap<>();
    params.put("y",3);
    Map<String,PythonRequestResponse<Long>> response = jepPythonEngine.eval(WorkerExecutionMode.ALL_WORKERS,
        1L,1L,"x=multiply(4,y)",
        "x", params,1000,TimeUnit.MILLISECONDS,false,Long.class);
    for (String aWorkerId: response.keySet()) {
      assertEquals(12L,response.get(aWorkerId).getPythonInterpreterResponse().getResponse());
    }
    assertEquals(jepPythonEngine.getNumWorkerThreads(),response.size()); // ensure all workers responded

    params = new HashMap<>();
    params.put("y",6);
    response = jepPythonEngine.eval(WorkerExecutionMode.ANY_WORKER,
      1L,1L,"x=multiply(4,y)",
      "x", params,1000,TimeUnit.MILLISECONDS,false,Long.class);
    for (String aWorkerId: response.keySet()) {
      assertEquals(24L,response.get(aWorkerId).getPythonInterpreterResponse().getResponse());
    }
    assertEquals(1,response.size()); // ensure all workers responded
    Thread.sleep(5000); // ensure subsequent tests are not impacted by busy flags from current test
  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testPostStartInterpreterLogic() throws Exception
  {
    JepPythonEngine pythonEngineForPostInit = new JepPythonEngine("unit-tests-jeppythonengine",
        5);
    List<String> commandsForPreInit = new ArrayList<>();
    commandsForPreInit.add("x=5");
    PythonRequestResponse<Void> aHistoryCommand = buildRequestResponseObjectForVoidPayload(
        PythonRequestResponse.PythonCommandType.GENERIC_COMMANDS);
    aHistoryCommand.getPythonInterpreterRequest().setGenericCommands(commandsForPreInit);
    aHistoryCommand.getPythonInterpreterRequest().setTimeOutInNanos(
        TimeUnit.NANOSECONDS.convert(1,TimeUnit.SECONDS));
    List<PythonRequestResponse> historyOfCommands = new ArrayList<>();
    historyOfCommands.add(aHistoryCommand);
    pythonEngineForPostInit.setCommandHistory(historyOfCommands);

    pythonEngineForPostInit.preInitInterpreter(new HashMap<String,Object>());
    pythonEngineForPostInit.startInterpreter();
    pythonEngineForPostInit.postStartInterpreter();

    HashMap<String,Object> params = new HashMap<>();
    params.put("y",4);
    Map<String,PythonRequestResponse<Long>> resultOfExecution = pythonEngineForPostInit.eval(
        WorkerExecutionMode.ALL_WORKERS, 1L,1L,"x=x+y","x",params,
        100,TimeUnit.MILLISECONDS,false,Long.class);
    assertEquals(pythonEngineForPostInit.getNumWorkerThreads(),resultOfExecution.size());
    for (String aWorkerId : resultOfExecution.keySet()) {
      assertEquals(9L, resultOfExecution.get(aWorkerId).getPythonInterpreterResponse().getResponse());
    }
  }

}


