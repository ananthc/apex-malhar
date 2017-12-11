package org.apache.apex.malhar.python.base.jep;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.requestresponse.PythonInterpreterRequest;
import org.apache.apex.malhar.python.base.requestresponse.PythonRequestResponse;
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

    PythonInterpreterRequest<Void> requestOne = buildRequestObjectForVoidGenericCommand(
        sequenceOfCommands,3,TimeUnit.SECONDS);
    PythonRequestResponse<Void> resultOne = interpreterWrapper.runCommands(1L,1L, requestOne);
    assertNotNull(resultOne);

    requestOne.setTimeUnit(TimeUnit.MILLISECONDS);
    requestOne.setTimeout(5);
    PythonRequestResponse<Void> resultTWo = interpreterWrapper.runCommands(1L,1L,requestOne);
    assertNull(resultTWo);

  }

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testDelayedResponseQueue() throws Exception
  {
    List<String> sequenceOfCommands = new ArrayList();
    sequenceOfCommands.add("import time");
    sequenceOfCommands.add("x=4;time.sleep(1)");

    PythonInterpreterRequest<Void> requestOne = buildRequestObjectForVoidGenericCommand(
        sequenceOfCommands,300,TimeUnit.MILLISECONDS);
    PythonRequestResponse<Void> resultOne = interpreterWrapper.runCommands(1L,1L,requestOne);

    HashMap<String,Object> evalParams  = new HashMap<>();
    evalParams.put("y", 2);

    PythonInterpreterRequest<Long> requestTwo = buildRequestObjectForLongEvalCommand(
        "x = x * y;time.sleep(1)","x",evalParams,10,TimeUnit.MILLISECONDS,false);
    PythonRequestResponse<Long> result = interpreterWrapper.eval(1L,1L,requestTwo);

    Thread.sleep(3000);

    // only the next command will result in the queue getting drained of previous requests hence below
    sequenceOfCommands = new ArrayList();
    sequenceOfCommands.add("x=5");

    PythonInterpreterRequest<Void> requestThree = buildRequestObjectForVoidGenericCommand(
        sequenceOfCommands,300,TimeUnit.MILLISECONDS);
    PythonRequestResponse<Void> resultThree = interpreterWrapper.runCommands(1L,1L,requestThree);

    assertFalse(delayedResponseQueueForWrapper.isEmpty());
    assertEquals(2, delayedResponseQueueForWrapper.drainTo(new ArrayList<>()));

  }

}
