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

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;
import datafu.test.pig.PigTests;
import junit.framework.Assert;

import datafu.test.pig.util.SchemaToString;

public class EvalFuncTests extends PigTests
{

  /**


  define SchemaToString datafu.test.pig.util.SchemaToString();

  data = LOAD 'input' AS (city:chararray, state:chararray, pop:int);

  with_schema = FOREACH data {
    GENERATE city, state, pop, SchemaToString( (city, state, pop) );
    };

  STORE with_schema INTO 'output';
   */
  @Multiline private String onReadyTest;

  @Test
  public void onReadyTest() throws Exception
  {
    PigTest test = createPigTestFromString(onReadyTest);

    writeLinesToFile("input",
      "New York\tNY\t8244910",
      "Austin\tTX\t820611",
      "San Francisco\tCA\t812826");

    test.runScript();

    assertOutput(test, "with_schema",
      "(New York,NY,8244910,{(city: chararray,state: chararray,pop: int)})",
      "(Austin,TX,820611,{(city: chararray,state: chararray,pop: int)})",
      "(San Francisco,CA,812826,{(city: chararray,state: chararray,pop: int)})");

  }
}
