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

package datafu.test.pig.urls;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

import datafu.test.pig.PigTests;

public class URLInfoTest extends PigTests
{
    /**
     define URLInfo datafu.pig.urls.URLInfo();

     data = load 'input' using PigStorage('|') as (i:int, url:chararray,expected:tuple(domain:chararray,host:chararray,protocol:chararray,path:chararray,port:int), queryparams:map[chararray]);
     data_out = foreach data generate i, URLInfo(url) as url_info, expected, queryparams;

     store data_out into 'output';
     */
    @Multiline private String urlInfoTest;

    @Test
    public void urlInfoTest() throws Exception
    {
        PigTest test = createPigTestFromString(urlInfoTest);

        String[] input = {
                "1|http://roger.bar.com/marketing/brand.html?x=foo|(bar.com,roger.bar.com,http,/marketing/brand.html,)|[x#foo]",
                "2|https://hello.world.org:90/products/data/data.html|(world.org,hello.world.org,https,/products/data/data.html,90)|)",
                "3|ftp://roger.bar.com/eng/hello.jsp?x=foo&y=bar|(bar.com,roger.bar.com,ftp,/eng/hello.jsp,)|[x#foo,y#bar])",
                "4|http://hello.world.org:90/products/data/data.html|(world.org,hello.world.org,http,/products/data/data.html,90)|)",
                "5|http://roger.bar.com/eng/hello.jsp?x=foo&y=bar&x=baz|(bar.com,roger.bar.com,http,/eng/hello.jsp,)|[x#foo,y#bar])"
        };

        writeLinesToFile("input",input);

        test.runScript();

        for (Tuple t : getLinesForAlias(test, "data_out"))
        {
            System.out.println("Validating case " + t.get(0));
            Tuple actual = (Tuple)t.get(1);
            Tuple expected = (Tuple)t.get(2);
            assertEquals(actual.get(0),expected.get(0));
            assertEquals(actual.get(1),expected.get(1));
            assertEquals(actual.get(2),expected.get(2));
            assertEquals(actual.get(3),expected.get(3));
            assertEquals(actual.get(4),expected.get(4));
            assertEquals(actual.get(5),t.get(3));
        }
    }
}
