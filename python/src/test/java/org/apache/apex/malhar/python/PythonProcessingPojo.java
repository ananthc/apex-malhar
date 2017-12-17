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
package org.apache.apex.malhar.python;

import org.apache.apex.malhar.python.base.util.NDArrayKryoSerializer;

import com.esotericsoftware.kryo.serializers.FieldSerializer;

import jep.NDArray;

public class PythonProcessingPojo
{

  private float y;

  private int x;

  @FieldSerializer.Bind(NDArrayKryoSerializer.class)
  private NDArray<float[]> numpyFloatArray;

  @FieldSerializer.Bind(NDArrayKryoSerializer.class)
  private NDArray<int[]> numpyIntArray;


  public float getY()
  {
    return y;
  }

  public void setY(float y)
  {
    this.y = y;
  }

  public int getX()
  {
    return x;
  }

  public void setX(int x)
  {
    this.x = x;
  }

  public NDArray<float[]> getNumpyFloatArray()
  {
    return numpyFloatArray;
  }

  public void setNumpyFloatArray(NDArray<float[]> numpyFloatArray)
  {
    this.numpyFloatArray = numpyFloatArray;
  }

  public NDArray<int[]> getNumpyIntArray()
  {
    return numpyIntArray;
  }

  public void setNumpyIntArray(NDArray<int[]> numpyIntArray)
  {
    this.numpyIntArray = numpyIntArray;
  }
}
