package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.test.JepPythonTestContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class InterpreterWrapperTest extends BaseJEPTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(InterpreterWrapperTest.class);

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testTimeOuts() throws Exception
  {
    List<String> sequenceOfCommands = new ArrayList();
    sequenceOfCommands.add("import time");
    sequenceOfCommands.add("time.sleep(1)");
    PythonRequestResponse<Void> resultOne = interpreterWrapper.runCommands(1L,1L,
        sequenceOfCommands,3, TimeUnit.SECONDS);
    assertNotNull(resultOne);

    PythonRequestResponse<Void> resultTWo = interpreterWrapper.runCommands(1L,1L,
        sequenceOfCommands,5, TimeUnit.MILLISECONDS);
    assertNull(resultTWo);

  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testDelayedResponseQueue() throws Exception
  {
    List<String> sequenceOfCommands = new ArrayList();
    sequenceOfCommands.add("import time");
    sequenceOfCommands.add("x=4;time.sleep(1)");
    PythonRequestResponse<Void> resultOne = interpreterWrapper.runCommands(1L,1L,
        sequenceOfCommands,300, TimeUnit.MILLISECONDS);
    HashMap<String,Object> evalParams  = new HashMap<>();
    evalParams.put("y", 2);
    PythonRequestResponse<Long> result = interpreterWrapper.eval(1L,1L,
        "x = x * y;time.sleep(1)","x",
        evalParams,10, TimeUnit.MILLISECONDS,false,Long.class);
    Thread.sleep(3000);
    // only the next command will result in the queue getting drained of previous requests hence below
    sequenceOfCommands = new ArrayList();
    sequenceOfCommands.add("x=5");
    resultOne = interpreterWrapper.runCommands(1L,1L,
      sequenceOfCommands,300, TimeUnit.MILLISECONDS);
    assertFalse(delayedResponseQueueForWrapper.isEmpty());
    assertEquals(2, delayedResponseQueueForWrapper.drainTo(new ArrayList<>()));

  }

}
