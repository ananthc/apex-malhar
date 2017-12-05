package org.apache.apex.malhar.python.test;

import java.io.File;

import org.junit.Rule;

import org.apache.commons.io.FileUtils;

public class BasePythonTest
{
  @Rule
  public PythonAvailabilityTestRule jepAvailabilityBasedTest = new PythonAvailabilityTestRule();

  protected void migrateFileFromResourcesFolderToTemp(String resourceFileName,String targetFilePath) throws Exception
  {
    ClassLoader classLoader = getClass().getClassLoader();
    File outFile = new File(targetFilePath);
    FileUtils.copyInputStreamToFile(classLoader.getResourceAsStream(resourceFileName), outFile);
  }
}
