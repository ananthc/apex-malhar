/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.test;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.base.jep.BasePythonJepTest;

/**
 * A Junit rule that helps in bypassing tests that cannot be done if the kudu cluster is not present.
 */
public class PythonAvailabilityTestRule implements TestRule
{

  private static final transient Logger LOG = LoggerFactory.getLogger(PythonAvailabilityTestRule.class);

  public static final String JEP_LIBRARY_PATH_ENV_SWITCH = "jepPythonInstalled";
  @Override
  public Statement apply(Statement base, Description description)
  {
    JepPythonTestContext testContext = description.getAnnotation(JepPythonTestContext.class);
    String jepLibraryEnabledValue = System.getProperty(JEP_LIBRARY_PATH_ENV_SWITCH);
    boolean runThisTest = false; // default is not to run the test if no annotation is specified
    if ( testContext != null) {
      if ((jepLibraryEnabledValue != null) && (testContext.jepPythonBasedTest())) {
        runThisTest = true;
      }
      if ((jepLibraryEnabledValue != null) && (!testContext.jepPythonBasedTest())) {
        runThisTest = false;
      }
      if ((jepLibraryEnabledValue == null) && (testContext.jepPythonBasedTest())) {
        runThisTest = true;
      }
      if ((jepLibraryEnabledValue == null) && (!testContext.jepPythonBasedTest())) {
        runThisTest = false;
      }
    }
    if (runThisTest) {
      // Run the original test
      try {
        BasePythonJepTest.testThreadInit();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return base;
    } else {
      // bypass the test altogether
      return new Statement()
      {

        @Override
        public void evaluate() throws Throwable
        {
          // Return an empty Statement object for those tests
        }
      };
    }

  }

}
