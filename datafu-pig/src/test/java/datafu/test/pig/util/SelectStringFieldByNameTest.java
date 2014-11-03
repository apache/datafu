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

import java.util.List;

import junit.framework.Assert;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.pigunit.PigTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class SelectStringFieldByNameTest extends PigTests
{
    /**

     define SelectStringFieldByName datafu.pig.util.SelectStringFieldByName();

     data = LOAD 'input' using PigStorage(',') AS (fieldName:chararray, text1:chararray, text2:chararray, text3:chararray);

     data2 = FOREACH data GENERATE SelectStringFieldByName(fieldName,*) as result;

     describe data2;

     data3 = FOREACH data2 GENERATE result;

     STORE data3 INTO 'output';
     */
    @Multiline private static String chooseFieldByValueTest;

    @Test
    public void chooseFieldByValueTest() throws Exception
    {
        PigTest test = createPigTestFromString(chooseFieldByValueTest);

        writeLinesToFile("input",
                "text1,hi,how,are",
                "text2,you,sir,today",
                "text3,bob,is,a",
                "text1,friend,of,mine",
                "text2,and,I,say",
                "text3,he,is,nice.");

        //test.runScript();

        assertOutput(test, "data3",
                "(hi)",
                "(sir)",
                "(a)",
                "(friend)",
                "(I)",
                "(nice.)");
    }
}
