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


public class DedupTests extends PigTests
{
    
  /**
  import 'datafu/dedup.pig';

  data = LOAD 'input' AS (key: int, val: chararray, dt: chararray);

  dedup_data = dedup(data, key, 'dt');

  STORE dedup_data INTO 'output';
   */
  @Multiline
  private String dedupTest;

  @Test
  public void dedupTest() throws Exception
  {
    PigTest test = createPigTestFromString(dedupTest);

    writeLinesToFile(	"input",
    					"1\ta\t20140201",
    					"2\td\t20140201",
    					"2\td2\t20140301");

    assertOutput(test, "dedup_data",
			"(1,a,20140201)",
			"(2,d2,20140301)");

  }

  /**
  import 'datafu/dedup.pig';

  data = LOAD 'input' AS (key: int, val: chararray, dt: chararray);

  dedup_data = dedup(data, '(key,val)', 'dt');

  STORE dedup_data INTO 'output';
   */
  @Multiline
  private String dedupWithMultipleKeysTest;

  @Test
  public void dedupWithMultipleKeysTest() throws Exception
  {
    PigTest test = createPigTestFromString(dedupWithMultipleKeysTest);

    writeLinesToFile(	"input",
    					"1\ta\t20140201",
    					"1\td\t20140202",
    					"2\tb\t20110201",
    					"2\tb\t20140201",
    					"3\tc\t20160201" );

    assertOutput(test, "dedup_data",
			"(1,a,20140201)",
			"(1,d,20140202)",
			"(2,b,20140201)",
			"(3,c,20160201)");
  }

}
