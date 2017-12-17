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
package org.apache.apex.examples.s3Output;

import org.apache.apex.malhar.lib.fs.s3.S3OutputModule;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.fs.FSInputModule;

/**
 * Application illustrating copy files from HDFS to S3 bucket.
 *
 * @since 3.8.0
 */
@ApplicationAnnotation(name = "HDFSToS3App")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    FSInputModule inputModule = dag.addModule("HDFSInputModule", new FSInputModule());
    S3OutputModule outputModule = dag.addModule("S3OutputModule", new S3OutputModule());

    dag.addStream("FileMetaData", inputModule.filesMetadataOutput, outputModule.filesMetadataInput);
    dag.addStream("BlocksMetaData", inputModule.blocksMetadataOutput, outputModule.blocksMetadataInput)
      .setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("BlocksData", inputModule.messages, outputModule.blockData).setLocality(DAG.Locality.CONTAINER_LOCAL);
  }
}
