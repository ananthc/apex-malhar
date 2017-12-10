package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.test.JepPythonTestContext;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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
    InterpreterWrapper candidateWrapperForExecution = jepPythonEngine.selectWorkerForCurrentCall(
        jepPythonEngine.getNumWorkerThreads() - 1);
    assertNotNull(candidateWrapperForExecution);
    assertFalse(overloadedWorkers.contains(candidateWrapperForExecution.getInterpreterId()));
    Thread.sleep(5000); // all the python workers must be free after this line

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
      aWrapper.runCommands(1L,1L,commands,5,TimeUnit.MILLISECONDS);
    }

    List<Long> params = new ArrayList<>();
    params.add(5L);
    params.add(25L);


  }

}


