package org.apache.apex.malhar.python.base.util;

import jep.NDArray;

public class NDimensionalArray<T>
{

  int[] dimensions;

  T data;

  int lengthOfSequentialArray;

  boolean signedFlag;

  public NDimensionalArray()
  {
  }

  public NDArray<T> toNDArray()
  {
    return new NDArray<T>(data,signedFlag,dimensions);
  }

  public int[] getDimensions()
  {
    return dimensions;
  }

  public void setDimensions(int[] dimensions)
  {
    this.dimensions = dimensions;
  }

  public int getLengthOfSequentialArray()
  {
    return lengthOfSequentialArray;
  }

  public void setLengthOfSequentialArray(int lengthOfSequentialArray)
  {
    this.lengthOfSequentialArray = lengthOfSequentialArray;
  }

  public boolean isSignedFlag()
  {
    return signedFlag;
  }

  public void setSignedFlag(boolean signedFlag)
  {
    this.signedFlag = signedFlag;
  }

  public T getData()
  {
    return data;
  }

  public void setData(T data)
  {
    this.data = data;
  }
}
