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
package org.apache.apex.examples.FileToJdbcApp;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.db.jdbc.JdbcFieldInfo;
import com.datatorrent.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import com.datatorrent.lib.db.jdbc.JdbcTransactionalStore;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

@ApplicationAnnotation(name = "FileToJdbcCsvParser")
/**
 * @since 3.8.0
 */
public class FileToJdbcCsvParser implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    // create operators
    FileReader fileReader = dag.addOperator("FileReader", FileReader.class);
    CsvParser csvParser = dag.addOperator("CsvParser", CsvParser.class);
    JdbcPOJOInsertOutputOperator jdbcOutputOperator = dag.addOperator("JdbcOutput", JdbcPOJOInsertOutputOperator.class);

    // configure operators
    String pojoSchema = SchemaUtils.jarResourceFileToString("schema.json");
    csvParser.setSchema(pojoSchema);

    jdbcOutputOperator.setFieldInfos(addFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutputOperator.setStore(outputStore);

    // add stream
    dag.addStream("Bytes", fileReader.byteOutput, csvParser.in);
    dag.addStream("POJOs", csvParser.out, jdbcOutputOperator.input);
  }

    /**
     * This method can be modified to have field mappings based on used defined
     * class
     */
  private List<JdbcFieldInfo> addFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("ACCOUNT_NO", "accountNumber", JdbcFieldInfo.SupportType.INTEGER, INTEGER));
    fieldInfos.add(new JdbcFieldInfo("NAME", "name", JdbcFieldInfo.SupportType.STRING, VARCHAR));
    fieldInfos.add(new JdbcFieldInfo("AMOUNT", "amount", JdbcFieldInfo.SupportType.INTEGER, INTEGER));
    return fieldInfos;
  }
}

