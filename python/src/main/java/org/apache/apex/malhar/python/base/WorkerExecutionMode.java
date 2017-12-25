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
package org.apache.apex.malhar.python.base;

/***
 * Used to specify if a given API invocation in the in-memory interpreter is going to be invoked for all the worker
 *  threads or just any one thread. {@link WorkerExecutionMode#ALL_WORKERS} is to be used when the command is
 *   resulting in a state of the interpreter which has to be used in subsequent calls. For example, deserializing a
 *    machine learning model can be used as a ALL_WORKERS model as the scoring can then be invoked across all worker
 *     threads if required. Conversely ANY_WORKER represents a state wherein any worker can be used to score.
 */
public enum WorkerExecutionMode
{
  ALL_WORKERS,
  ANY_WORKER
}
