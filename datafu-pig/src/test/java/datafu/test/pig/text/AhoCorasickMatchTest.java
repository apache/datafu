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
     *
     * data = LOAD 'input' AS (text: chararray, dictionary: bag{t:tuple(word:chararray)});
     *
     * dump data;
     *
     * data2 = FOREACH data GENERATE AhoCorasickMatch(text, dictionary) AS hits;
     *
     * dump data2;
     *
     * STORE data2 INTO 'output';
     */
    @Multiline
    private String ahoCorasickMatchTest;

    @Test
    public void ahoCorasickMatchTest() throws Exception {
        PigTest test = createPigTestFromString(ahoCorasickMatchTest);

        writeLinesToFile("input",
                "This is a sentence. This is another sentence\t{(sent),(an),(his),(ten),(apple)}",
                "Yet another sentence. One more just for luck\t{(sent),(apple),(ten)}");

        assertOutput(test, "data2",
                "({(an,28,29),(his,1,3),(his,21,23),(sent,10,13),(sent,36,39),(ten,13,15),(ten,39,41)})",
                "({(sent,12,15),(ten,15,17)})");
    }

    /**
     * define AhoCorasickMatch datafu.pig.text.AhoCorasickMatch('REMOVE_OVERLAPS', 'ONLY_WHOLE_WORDS', 'CASE_INSENSITIVE');
     *
     * data = LOAD 'input' AS (text: chararray, dictionary: bag{t:tuple(word:chararray)});
     *
     * dump data;
     *
     * data2 = FOREACH data GENERATE AhoCorasickMatch(text, dictionary) AS hits;
     *
     * dump data2;
     *
     * STORE data2 INTO 'output';
     */
    @Multiline
    private String ahoCorasickMatchTest2;

    @Test
    public void ahoCorasickMatchTest2() throws Exception {
        PigTest test = createPigTestFromString(ahoCorasickMatchTest2);

        writeLinesToFile("input",
                "This is a sentence. This is another sentence\t{(sent),(an),(his),(ten),(apple),(a),(is)}",
                "Yet another sentence. One more just for luck\t{(sent),(apple),(ten),(for)}");

        assertOutput(test, "data2",
                "({(a,8,8),(a,28,28),(an,28,29),(his,1,3),(his,21,23),(is,2,3),(is,5,6),(is,22,23),(is,25,26),(sent,10,13),(sent,36,39),(ten,13,15),(ten,39,41)})",
                "({(for,36,38),(sent,12,15),(ten,15,17)})");
    }
}