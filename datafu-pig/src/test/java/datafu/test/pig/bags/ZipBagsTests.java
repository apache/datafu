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

package datafu.test.pig.bags;

import datafu.test.pig.PigTests;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

public class ZipBagsTests extends PigTests {

    /**
     DEFINE ZipBags datafu.pig.bags.ZipBags();

     data = LOAD 'input' AS (B1: bag {T:tuple(a:INT,b:INT)}, B2: bag {U:tuple(c:INT,d:INT)});

     dump data;

     describe data;

     zipped = FOREACH data GENERATE ZipBags(B1,B2);

     describe zipped;

     dump zipped;

     STORE zipped INTO 'output';

     */
    @Multiline
    private String zipBagsTest;

    @Test
    public void zipBagsTest() throws Exception
    {
        PigTest test = createPigTestFromString(zipBagsTest);
        writeLinesToFile("input", "{(1,2),(3,4),(5,6)}\t{(7,8),(9,10),(11,12)}");
        test.runScript();
        assertOutput(test, "zipped", "({(1,2,7,8),(3,4,9,10),(5,6,11,12)})");
    }

    @Test(expectedExceptions = FrontendException.class)
    public void zipUnevenBagsExceptionTest() throws Exception {
        PigTest test = createPigTestFromString(zipBagsTest);
        writeLinesToFile("input", "{(1,2),(3,4),(5,6),(20,40)}\t{(7,8),(9,10)}");
        test.runScript();
    }

    @Test
    public void zipUnevenBagsTest() throws Exception {
        PigTest test = createPigTestFromString(zipBagsTest);
        writeLinesToFile("input", "{(1,2),(3,4),(5,6))}\t{(7,8),(9,10),(11,12),(1,2)}");
        test.runScript();
        assertOutput(test, "zipped", "({(1,2,7,8),(3,4,9,10),(5,6,11,12)})");
    }

    /**
     DEFINE ZipBags datafu.pig.bags.ZipBags();

     data = LOAD 'input' AS (B1: bag {T:tuple(a:INT,b:INT)}, B2: bag {U:tuple(a:INT,d:INT)});

     describe data;

     zipped = FOREACH data GENERATE ZipBags(B1,B2);

     describe zipped;

     STORE zipped INTO 'output';
     */
    @Multiline
    private String duplicateAliasTest;

    @Test(expectedExceptions = FrontendException.class)
    public void duplicateAliasTest() throws Exception
    {
        PigTest test = createPigTestFromString(duplicateAliasTest);
        writeLinesToFile("input", "{(1,2),(3,4),(5,6)}\t{(7,8),(9,10),(11,12)}");
        test.runScript();
    }
}
