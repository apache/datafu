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

import java.io.IOException;
import java.util.Arrays;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.pigunit.PigTest;
import org.testng.Assert;
import org.testng.annotations.Test;

import datafu.pig.util.TupleDiff;
import datafu.test.pig.PigTests;

public class TupleDiffTest extends PigTests {

    final TupleFactory tf = TupleFactory.getInstance();
    final BagFactory bf = BagFactory.getInstance();

    /**
        import 'datafu/diff_macros.pig';
     */
    @Multiline private static String macro;

    /**  
        d1 = LOAD 'd1' using PigStorage(',') AS (key:chararray, text1:chararray, text2:chararray, num:int, last_update:long);
        d2 = LOAD 'd2' using PigStorage(',') AS (key:chararray, text1:chararray, text2:chararray, num:int, last_update:long);
     */
    @Multiline private static String load;

    private void prepareFiles() throws IOException {
        writeLinesToFile("d1",
                "key1,hi,how,1,22",
                "key2,you,sir,2,33",
                "key4,bob,is,4,55");

        writeLinesToFile("d2",
                "key1,hi,how,1,23",
                "key3,you,sir,3,44",
                "key4,bob,is not,6,55");
    }

    /**
        flat_diff = diff_macro(d1, d2, 'key', 'last_update');
     */
    @Multiline private static String flatDiffScript;

    @Test
    public void flatDiffScriptTest() throws Exception
    {
    	prepareFiles();
    	
        PigTest test = createPigTestFromString(load + macro + flatDiffScript);

        assertOutput(test, "flat_diff",
                "(missing,(key2,you,sir,2,33),)",
                "(added,,(key3,you,sir,3,44))",
                "(changed text2 num,(key4,bob,is,4,55),(key4,bob,is not,6,55))");
    }

    /**

        td1 = foreach d1 generate key, TOTUPLE(text1, text2) AS tup, num, last_update;
        td2 = foreach d2 generate key, TOTUPLE(text1, text2) AS tup, num, last_update;

        single_tuple_diff = diff_macro(td1, td2, 'key', 'last_update');
     */
    @Multiline private static String embeddedDiffScript;

    @Test(dependsOnMethods = {"flatDiffScriptTest"})
    public void embeddedDiffScriptTest() throws Exception
    {
        PigTest test = createPigTestFromString(load + embeddedDiffScript);

        assertOutput(test, "single_tuple_diff",
                "(missing,(key2,(you,sir),2,33),)",
                "(added,,(key3,(you,sir),3,44))",
                "(changed tup(text2) num,(key4,(bob,is),4,55),(key4,(bob,is not),6,55))");

    }

    /**
        td1 = foreach d1 generate key, text1, TOTUPLE(text2, num) AS innerTuple, last_update;
        td2 = foreach d2 generate key, text1, TOTUPLE(text2, num) AS innerTuple, last_update;

        td1 = foreach td1 generate key, TOTUPLE(text1, innerTuple) AS outerTuple, last_update;
        td2 = foreach td2 generate key, TOTUPLE(text1, innerTuple) AS outerTuple, last_update;

        double_tuple_diff = diff_macro(td1, td2, 'key', 'last_update');
     */
    @Multiline private static String doubleEmbeddedDiffScript;

    @Test(dependsOnMethods = {"flatDiffScriptTest"})
    public void doubleEmbeddedDiffScriptTest() throws Exception
    {
        PigTest test = createPigTestFromString(load + doubleEmbeddedDiffScript);

        assertOutput(test, "double_tuple_diff",
                "(missing,(key2,(you,(sir,2)),33),)",
                "(added,,(key3,(you,(sir,3)),44))",
                "(changed outerTuple(innerTuple(text2) outerTuple(innerTuple(num)),(key4,(bob,(is,4)),55),(key4,(bob,(is not,6)),55))");

    }

    final Tuple TUPLE1 = tf.newTuple(Arrays.asList("first", "second"));
    final Tuple TUPLE1COPY = tf.newTuple(Arrays.asList("first", "second"));
    final Tuple TUPLE2 = tf.newTuple(Arrays.asList("first", "second_prime", "third"));
    final Tuple TUPLE3 = tf.newTuple(Arrays.asList("first_prime", "second"));

    @Test
    public void tupleCompare() throws Exception {
        TupleDiff tupleDiff = new TupleDiff();

        Tuple tuple = tf.newTuple(Arrays.asList(TUPLE1, TUPLE1COPY));
        Assert.assertEquals(tupleDiff.exec(tuple), null, "Equal tuples should not generate diff");

        tuple = tf.newTuple(Arrays.asList(TUPLE1, TUPLE1COPY, "nonexisting"));
        Assert.assertEquals(tupleDiff.exec(tuple), null, "Non-existing ignore fields shouldn't have an effect");

        tuple = tf.newTuple(Arrays.asList(TUPLE1, TUPLE1COPY, "0"));
        Assert.assertEquals(tupleDiff.exec(tuple), null, "Ignoring fields which are equal shouldn't have an effect");

        tuple = tf.newTuple(Arrays.asList(TUPLE1, null));
        Assert.assertEquals(tupleDiff.exec(tuple), "missing", "Failed to identify missing tuple");

        tuple = tf.newTuple(Arrays.asList(null, TUPLE1));
        Assert.assertEquals(tupleDiff.exec(tuple), "added", "Failed to identify added tuple");

        // new tuple has more fields than old
        tuple = tf.newTuple(Arrays.asList(TUPLE1, TUPLE2));
        Tuple tupleWithPositionalIgnore = tf.newTuple(Arrays.asList(TUPLE1, TUPLE2, "2"));
        Tuple tupleWithFieldNameIgnore = tf.newTuple(Arrays.asList(TUPLE1, TUPLE2, "f2"));

        Assert.assertEquals(tupleDiff.exec(tuple), "changed 1 2", "Failed to identify changed fields by position");
        Assert.assertEquals(tupleDiff.exec(tupleWithPositionalIgnore), "changed 1", "Failed to ignore field");

        Schema twoFieldsSchema = new Schema(Arrays.asList(new FieldSchema("f1", DataType.CHARARRAY),new FieldSchema("f2", DataType.CHARARRAY)));
        Schema threeFieldsSchema = new Schema(Arrays.asList(new FieldSchema("f1", DataType.CHARARRAY),new FieldSchema("f2", DataType.CHARARRAY),new FieldSchema("f3", DataType.CHARARRAY)));

        tupleDiff.setInputSchema(new Schema(Arrays.asList(new FieldSchema("t1",twoFieldsSchema), new FieldSchema("t2",threeFieldsSchema))));

        Assert.assertEquals(tupleDiff.exec(tuple), "changed f2 null/f3", "Different field names should both appear in result");
        Assert.assertEquals(tupleDiff.exec(tupleWithFieldNameIgnore), "changed null/f3", "Should have ignored field 'f2'");
       
        // old tuple has more fields than new
        tuple = tf.newTuple(Arrays.asList(TUPLE2, TUPLE1));
        tupleDiff = new TupleDiff();

        Assert.assertEquals(tupleDiff.exec(tuple), "changed 1 2", "Should have identified changed field 1 and missing field 2");
       
        tupleDiff.setInputSchema(new Schema(Arrays.asList(new FieldSchema("t1",threeFieldsSchema), new FieldSchema("t2",twoFieldsSchema))));
        Assert.assertEquals(tupleDiff.exec(tuple), "changed f2 f3/null", "Should have identified changed field f2 and missing field f3");
    }

    @Test
    public void simpleBagCompare() throws Exception {
        TupleDiff tupleDiff = new TupleDiff();

        Tuple tuple = tf.newTuple(Arrays.asList(
                tf.newTuple(bf.newDefaultBag(Arrays.asList(TUPLE1, TUPLE3))),
                tf.newTuple(bf.newDefaultBag(Arrays.asList(TUPLE2, TUPLE3)))));

        Assert.assertEquals(tupleDiff.exec(tuple), "changed 0", "Should identify changed bag by position (0)");

        Schema tupleSchema = new Schema(Arrays.asList(new FieldSchema("f1", DataType.BAG)));
        tupleDiff.setInputSchema(new Schema(Arrays.asList(new FieldSchema("t1",tupleSchema), new FieldSchema("t2",tupleSchema))));

        Assert.assertEquals(tupleDiff.exec(tuple), "changed f1", "Should identify changed bag f1 by name");

        tuple = tf.newTuple(Arrays.asList(
                tf.newTuple(bf.newDefaultBag(Arrays.asList(TUPLE1, TUPLE3))),
                tf.newTuple(bf.newDefaultBag(Arrays.asList(TUPLE1COPY, TUPLE3)))));

        Assert.assertEquals(tupleDiff.exec(tuple), null, "Equal bags shouldn't generate a result");
    }
}