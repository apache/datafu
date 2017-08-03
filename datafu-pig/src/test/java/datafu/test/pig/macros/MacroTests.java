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

package datafu.test.pig.macros;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class MacroTests extends PigTests
{
  /**

  import 'datafu/count_macros.pig';

  data = LOAD 'input' AS (id:chararray, num:int);

  cnt = count_distinct_keys(data, 'id');

  STORE cnt INTO 'output';

   */
  @Multiline
  private String countDistinctTest;

  @Test
  public void countDistinctTest() throws Exception
  {
    PigTest test = createPigTestFromString(countDistinctTest);

    writeLinesToFile("input",
                     "A1\t1","A1\t4","A1\t4","A1\t4",
                     "A2\t4","A2\t4",
                     "A3\t3","A3\t1","A3\t77",
                     "A4\t3","A4\t3","A4\t59","A4\t29",
                     "A5\t4",
                     "A6\t3","A6\t55","A6\t1",
                     "A7\t39","A7\t27","A7\t85",
                     "A8\t4","A8\t45",
                     "A9\t92", "A9\t42","A9\t1","A9\t0",
                     "A10\t7","A10\t23","A10\t1","A10\t41","A10\t52");

    test.runScript();

    assertOutput(test, "cnt", "(10)");
  }

  /**

  import 'datafu/count_macros.pig';

  data = LOAD 'input' AS (id:chararray, num:int);

  cnt = count_all_non_distinct(data);

  STORE cnt INTO 'output';

   */
  @Multiline
  private String countTest;

  @Test
  public void countTest() throws Exception
  {
    PigTest test = createPigTestFromString(countTest);

    writeLinesToFile("input",
                     "A1\t1","A1\t4","A1\t4","A1\t4",
                     "A2\t4","A2\t4",
                     "A3\t3","A3\t1","A3\t77",
                     "A4\t3","A4\t3","A4\t59","A4\t29",
                     "A5\t4",
                     "A6\t3","A6\t55","A6\t1",
                     "A7\t39","A7\t27","A7\t85",
                     "A8\t4","A8\t45",
                     "A9\t92", "A9\t42","A9\t1","A9\t0",
                     "A10\t7","A10\t23","A10\t1","A10\t41","A10\t52");

    test.runScript();

    assertOutput(test, "cnt", "(31)");
  }

}
