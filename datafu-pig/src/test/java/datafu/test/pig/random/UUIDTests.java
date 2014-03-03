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
package datafu.test.pig.random;

import datafu.test.pig.PigTests;
import junit.framework.Assert;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;

import java.util.*;

import static org.testng.Assert.assertTrue;

public class UUIDTests extends PigTests
{
    /**

     define RandomUUID datafu.pig.random.RandomUUID();

     data = LOAD 'input' AS (key: chararray);
     DUMP data

     data2 = FOREACH data GENERATE key, RandomUUID() as val;
     DUMP data2

     STORE data2 INTO 'output';
     */
    @Multiline private String randomUUIDTest;

    /**
     * Test the RandomUUID UDF.  The main purpose is to make sure it can be used in a Pig script.
     * Also the range of length of values is tested.
     *
     * @throws Exception
     */
    @Test
    public void randomUUIDTest() throws Exception
    {
        PigTest test = createPigTestFromString(randomUUIDTest);

        writeLinesToFile("input",
                "input1",
                "input2",
                "input3");

        List<Tuple> tuples = getLinesForAlias(test, "data2", true);
        Set<UUID> set = new HashSet<UUID>();
        for (Tuple tuple : tuples)
        {
            set.add(UUID.fromString((String)tuple.get(1)));
        }
        Assert.assertEquals(set.size(), 3);
    }
}
