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

package datafu.test.pig.stats;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class TFIDFTests extends PigTests

{
  /**

  import 'datafu/tf_idf.pig';

  raw_documents = LOAD 'input' AS (id:chararray, text:chararray);

  -- Compute topics per document via Macro
  vectors = DataFu_NlpTFIDF(raw_documents, 100);

  STORE vectors INTO 'output';

   */
  @Multiline
  private String tfidfTest;

  @Test
  public void simpleTFIDFTest() throws Exception
  {
    PigTest test = createPigTestFromString(tfidfTest);

    writeLinesToFile(	"input",
						"text\tthis is a a sample",
						"text2\tthis is another another example example example");

    test.runScript();

    String expected[] = {
    		"(text,{(a,0.6931471805599453),(sample,0.5198603854199589),(this,0.0),(is,0.0)})",
    		"(text2,{(example,0.6931471805599453),(another,0.5776226780098154),(this,0.0),(is,0.0)})"};
    
    assertOutput(test, "vectors", expected);
  }

}
