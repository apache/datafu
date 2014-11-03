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
package datafu.pig.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Selects the value for a field within a tuple using that field's name.
 *
 * <p>
 * Example:
 * <pre>
 * {@code
 * define SelectStringFieldByName datafu.pig.util.SelectStringFieldByName();
 *
 * -- input:
 * -- ("text1", "hi", "how", "are")
 * input = LOAD 'input' AS (fieldName:chararray, text1:chararray, text2:chararray, text3:chararray);
 *
 * -- output:
 * -- ("hi")
 * outfoo = FOREACH input GENERATE SelectStringFieldByName(fieldName, *) as value;
 * }
 * </pre>
 */

public class SelectStringFieldByName extends EvalFunc<String>
{
    @Override
    public String exec(Tuple input) throws IOException
    {
        if(input.size() < 2) {
            throw new IllegalArgumentException("Less then two arguments!");
        }

        String fieldNameToReturn = input.get(0).toString();
        if(fieldNameToReturn == null || fieldNameToReturn == "") {
            return null;
        }

        Schema inputSchema = getInputSchema();
        String matchField = null;
        for(int i=1; i < input.size(); i++)
        {
            Schema.FieldSchema fieldSchema = inputSchema.getField(i);
            if(fieldSchema.alias.equals(fieldNameToReturn)) {
                matchField = (String)input.get(i);
                break;
            }
        }
        return matchField;
    }
}
