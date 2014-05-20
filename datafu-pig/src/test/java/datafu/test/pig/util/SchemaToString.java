/*
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

package datafu.test.pig.util;

import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import datafu.pig.util.SimpleEvalFunc;

/**
 * Simple UDF for testing purposes.
 *
 * Captures a value at front-end (launch) time, namely a string for the input
 * schema, and returns its value (ignoring the actual input it characterizes)
 */
public class SchemaToString extends SimpleEvalFunc<String> {
  private String schema_string = null;

  /**
   * Hook to capture the input schema string that we will spit out later
   */
  @Override
  public void onReady(Schema in_schema, Schema out_schema) {
    String sch_str = in_schema.toString();
    sch_str = sch_str.replaceFirst("[\\w\\.]+: ", "");
    getInstanceProperties().put("schema_string", sch_str);
    super.onReady(in_schema, out_schema);
  }

  /*
   * @param  a tuple
   * @return the schema of the tuple
   */
  public String call(Tuple tup)
  {
    if (schema_string == null) { schema_string = (String)getInstanceProperties().get("schema_string"); }
    return schema_string;
  }
}
