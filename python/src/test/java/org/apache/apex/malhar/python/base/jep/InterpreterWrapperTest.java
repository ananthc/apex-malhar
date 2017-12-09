package org.apache.apex.malhar.python.base.jep;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.PythonRequestResponse;
import org.apache.apex.malhar.python.test.JepPythonTestContext;

import static org.junit.Assert.assertNotNull;

public class InterpreterWrapperTest extends BaseJEPTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(InterpreterWrapperTest.class);

  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void runCommandsAsWrapper() throws Exception
  {
    File tempFile = File.createTempFile("apexpythonwrappertest-", ".txt");
    tempFile.deleteOnExit();
    String filePath = tempFile.getAbsolutePath();

    List<String> sequenceOfCommands = new ArrayList();
    sequenceOfCommands.add("import time");
    sequenceOfCommands.add("time.sleep(1)");
    PythonRequestResponse<Void> resultOne = interpreterWrapper.runCommands(1L,1L,
        sequenceOfCommands,3, TimeUnit.SECONDS);
    assertNotNull(resultOne);


  }

  @Test
  public void executeMethodCallAsWrapper() throws Exception
  {
  }

  @Test
  public void executeScriptAsWrapper() throws Exception
  {
  }

  @Test
  public void evalAsWrapper() throws Exception
  {
  }

  private void runCommandsForCreatingFile() throws Exception
  {

  }

}
