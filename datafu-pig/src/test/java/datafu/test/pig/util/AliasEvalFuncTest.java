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

import static org.testng.Assert.*;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.io.IOException;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import org.testng.Assert;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;
import datafu.pig.util.AliasableEvalFunc;

public class AliasEvalFuncTest extends PigTests
{
  static class ReportBuilder extends AliasableEvalFunc<DataBag> {
    static final String ORDERED_ROUTES = "orderedRoutes";

    public DataBag exec(Tuple input) throws IOException {
       DataBag inputBag = getBag(input, ORDERED_ROUTES);
       DataBag outputBag = BagFactory.getInstance().newDefaultBag();
       for(Iterator<Tuple> tupleIter = inputBag.iterator(); tupleIter.hasNext(); ) {
           outputBag.add(tupleIter.next());
       }
       return outputBag;
    }

    public Schema getOutputSchema(Schema input) {
       try {
            Schema bagSchema = input.getField(0).schema;
            Schema outputSchema = new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                                                                    .getName()
                                                                    .toLowerCase(), input),
                                                                    bagSchema,
                                                                    DataType.BAG));
            return outputSchema;
       } catch (Exception ex) {
            return null;
       }
    }
  }

  @Test
  public void getBagTest() throws Exception
  {
     ReportBuilder udf = new ReportBuilder();
     udf.setUDFContextSignature("test");
     List<Schema.FieldSchema> fieldSchemaList = new ArrayList<Schema.FieldSchema>();
     fieldSchemaList.add(new Schema.FieldSchema("msisdn", DataType.LONG));
     fieldSchemaList.add(new Schema.FieldSchema("ts", DataType.INTEGER));
     fieldSchemaList.add(new Schema.FieldSchema("center_lon", DataType.DOUBLE));
     fieldSchemaList.add(new Schema.FieldSchema("center_lat", DataType.DOUBLE));
     Schema schemaTuple = new Schema(fieldSchemaList);
     Schema schemaBag = new Schema(new Schema.FieldSchema(ReportBuilder.ORDERED_ROUTES, schemaTuple, DataType.BAG));
     udf.outputSchema(schemaBag);

     Tuple inputTuple = TupleFactory.getInstance().newTuple();
     DataBag inputBag = BagFactory.getInstance().newDefaultBag();
     inputBag.add(TupleFactory.getInstance().newTuple(Arrays.asList(71230000000L, 1382351612, 10.697, 20.713)));
     inputTuple.append(inputBag);
     DataBag outputBag = udf.exec(inputTuple);
     Assert.assertEquals(inputBag, outputBag);
  }
}
