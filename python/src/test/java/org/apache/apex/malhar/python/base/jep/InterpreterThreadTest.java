package org.apache.apex.malhar.python.base.jep;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.test.JepPythonTestContext;

public class InterpreterThreadTest extends BaseJEPTest
{
  private static final transient Logger LOG = LoggerFactory.getLogger(InterpreterThreadTest.class);


  @JepPythonTestContext(jepPythonBasedTest = true)
  @Test
  public void testImportCommand() throws Exception
  {
    LOG.info("Loaded the class..");
  }
}
