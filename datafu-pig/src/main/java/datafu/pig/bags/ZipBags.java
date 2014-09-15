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

package datafu.pig.bags;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;
import java.util.*;

/**
 * This udf takes any number of bags and allows you to zip them into one bag
 * with the tuples inside each bag concatenated to each other.
 * Example:
 * <pre>
 * {@code
 * -- input:
 * -- ({(1,2),(3,4),(5,6)},{(7,8),(9,10),(11,12)})
 * input = LOAD 'input' AS (OUTER: tuple(B1: bag {a:INT,b:INT}, B2: bag{c:INT,d:INT}));
 *
 * -- output:
 * -- ({(1,2,7,8),(3,4,9,10),(5,6,11,12)})
 * k
 * output = FOREACH input GENERATE ZipBags(B1,B2);
 * }
 * </pre>
 * For this to work as expected each bag should be the same length. It will run as long as
 * the first bag is the shortest however this may not be the desired behavior.
 */
public class ZipBags extends EvalFunc<DataBag> {


    @Override
    public DataBag exec(Tuple input) throws IOException {
        DataBag outputBag = new DefaultDataBag();
        //All bags should have the same length for this to work as expected
        List<Iterator<Tuple>> bagIterators = new ArrayList<Iterator<Tuple>>();
        for (int i = 0; i < input.size(); ++i) {
            Object obj = input.get(i);
            if (obj instanceof DataBag) {
                DataBag bag = (DataBag)obj;
                bagIterators.add(bag.iterator());
            }
            else {
                throw new IllegalArgumentException("Expected all fields to be bags");
            }
        }
        while (bagIterators.get(0).hasNext()) {
            Tuple nextTuple = new DefaultTuple();
            for (Iterator<Tuple> iter : bagIterators) {
                if (!iter.hasNext()) {
                    throw new IllegalArgumentException("The first bag must be the shortest one");
                }
                Tuple tempTuple = iter.next();
                //This loop is to flatten the tuples stored in the DataBag
                for (int i = 0; i < tempTuple.size(); ++i) nextTuple.append(tempTuple.get(i));
            }
            outputBag.add(nextTuple);
        }
        return outputBag;
    }

    @Override
    public Schema outputSchema(Schema input) {
        Schema bagTupleSchema = new Schema();
        Set<String> aliasSet = new HashSet<String>();
        for (FieldSchema schema : input.getFields()) { //Each field should be a bag
            if (schema.schema == null) throw new RuntimeException("Inner bag schemas are null");
            for (FieldSchema innerBagTuple : schema.schema.getFields()) {
                for (FieldSchema tupleField : innerBagTuple.schema.getFields()) {
                    if (!aliasSet.add(tupleField.alias)) {
                        throw new RuntimeException("Duplicate field alias specified");
                    }
                    bagTupleSchema.add(tupleField);
                }
            }
        }
        try {
            return new Schema(new FieldSchema("zipped",bagTupleSchema, DataType.BAG));
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }
}
