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

package datafu.pig.text;

import org.apache.pig.data.*;

import java.io.IOException;
import java.util.Collection;

import org.ahocorasick.trie.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pig.Accumulator;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Trie based substring search.  See http://en.wikipedia.org/wiki/Trie
 * and http://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_string_matching_algorithm
 * <p>
 * Example:
 * <pre>
 * {@code
 * define AhoCorasickMatch datafu.pig.text.AhoCorasickMatch();
 *
 * -- input:
 * -- (Hello mister. How are you, sport?,{(hello),(mister),(how),(are),(you)})
 * input = LOAD 'input' AS (inputText:chararray,dictionary:bag{T:tuple(word:chararray)});
 *
 * -- output:
 * -- ()
 * output = FOREACH input GENERATE AhoCorasickMatch(inputText,dictionary);
 * }
 * </pre>
 */
public class AhoCorasickMatch extends EvalFunc<DataBag>{

    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag outputBag = bagFactory.newDistinctBag();
        Trie trie = new Trie().removeOverlaps();

        // Must have both arguments specified
        if (input.size() != 2) {
            throw new IOException();
        }

        // chararray input text
        String inputString = input.get(0).toString();
        if (inputString == null || inputString == "") {
            return null;
        }

        // Bag of strings search dictionary
        Object o = input.get(1);
        if (!(o instanceof DataBag))
            throw new RuntimeException("parameters must be databags");

        // Build Trie dictionary
        DataBag inputBag = (DataBag) o;
        for (Tuple elem : inputBag) {
            String matchWord = elem.get(0).toString();
            trie.addKeyword(matchWord);
        }

        // Run Aha Corsick operation
        Collection<Emit> emits = trie.parseText(inputString);

        // Build a 3-tuple for each emit, (word, start, end), ex.: (bob, 10, 12)
        for(Emit emit : emits) {
            Tuple t = tupleFactory.newTuple(3);

            t.set(0, emit.getKeyword());
            t.set(1, emit.getStart());
            t.set(2, emit.getEnd());

            outputBag.add(t);
        }
        return outputBag;
    }

    @Override
    public Schema outputSchema(Schema input)
    {
        try {
            // Verify that input schema contains a chararray field, and a field holding a bag of chararray tuples to match
            Schema.FieldSchema inputFirstSchema = input.getField(0);
            Schema.FieldSchema inputSecondSchema = input.getField(1);

            // Validate first field that holds our string to search against
            if(inputFirstSchema.type != DataType.CHARARRAY)
            {
                throw new RuntimeException("Expected CHARARRAY as first input");
            }

            // Validate second field that holds our bag of tupples with a single chararray field
            if(inputSecondSchema.type != DataType.BAG)
            {
                throw new RuntimeException("Expected BAG as second input");
            }
            Schema inputBagSchema = inputSecondSchema.schema;
            if (inputBagSchema.getField(0).type != DataType.TUPLE)
            {
                throw new RuntimeException("Expected a BAG of TUPLEs as second input");
            }

            Schema.FieldSchema tupleSchema = inputBagSchema.getField(0);
            if (tupleSchema.type != DataType.CHARARRAY)
            {
                throw new RuntimeException(String.format("Expected secodnd input to contain a BAG of TUPLES with a single CHARARRAY field, but instead found %s",
                        DataType.findTypeName(inputBagSchema.getField(0).type)));
            }

            // Build output Schema - a bag of tuples with three fields: keyword/start/end
            Schema outputTupleSchema = new Schema();
            outputTupleSchema.add(new Schema.FieldSchema("keyword", DataType.CHARARRAY));
            outputTupleSchema.add(new Schema.FieldSchema("start", DataType.INTEGER));
            outputTupleSchema.add(new Schema.FieldSchema("end", DataType.INTEGER));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
            .getName()
            .toLowerCase(), input),
            outputTupleSchema,
            DataType.BAG));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
