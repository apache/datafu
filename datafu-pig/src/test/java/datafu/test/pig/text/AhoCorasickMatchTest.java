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

package datafu.test.pig.text;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;


public class AhoCorasickMatchTest extends PigTests {
    /**
     * define AhoCorasickMatch datafu.pig.text.AhoCorasickMatch();
     * <p/>
     * data = LOAD 'input' AS (text: chararray, dictionary: bag{t:tuple(word:chararray)});
     * <p/>
     * dump data;
     * <p/>
     * data2 = FOREACH data GENERATE AhoCorasickMatch(text, dictionary) AS matches;
     * <p/>
     * dump data2;
     * <p/>
     * STORE data2 INTO 'output';
     */
    @Multiline
    private String ahoCorasickMatchTest;

    @Test
    public void ahoCorasickMatchTest() throws Exception {
        PigTest test = createPigTestFromString(ahoCorasickMatchTest);

        writeLinesToFile("input",
                "(This is a sentence. This is another sentence., {(sent),(an),(his),(ten),(apple)})",
                "(Yet another sentence. One more just for luck., {(sent),(apple),(ten)})");

        System.err.print("ERROR: FOO");
        System.err.print("TEST: " + test + "\n");
        System.out.print("TEST: " + test + "\n");

        assertOutput(test, "data2",
                "({(sent,10,13),(an,18,19),(his,12,15),(ten,20,22)})",
                "({(sent,10,13),(ten,20,22)})");
    }
}