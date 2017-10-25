package org.apache.apex.malhar.python.base;

/**
 * An exception used to denote issues when using the ApexPython Engine.
 */
public class ApexPythonInterpreterException extends Exception
{
  public ApexPythonInterpreterException(Throwable cause)
  {
    super(cause);
  }
}
