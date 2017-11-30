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

package datafu.pig.sampling;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.math.random.RandomDataImpl;
import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.util.SortedSet;
import java.util.TreeSet;
/**
 * Scalable uniform random sampling.
 *
 * <p>
 * This UDF implements a uniform random sampling algorithm
 * </p>
 *
 * <p>
 * It takes a bag of n items and either a fraction (p) or an exact number (k) of
 * the items to be selected as the input, and returns a bag of k or ceil(p*n)
 * items uniformly sampled.
 * </p>
 *
 * <pre>
 * DEFINE URS datafu.pig.sampling.URandomSample();
 *
 * item    = LOAD 'input' AS (x:double);
 * sampled = FOREACH (GROUP items ALL) GENERATE FLATTEN(URS(items, p||k));
 * </pre>
 *
 */

public class UniformRandomSample extends AlgebraicEvalFunc<DataBag> {
    /**
     * Prefix for the output bag name.
     */
    public static final String OUTPUT_BAG_NAME_PREFIX = "URS";

    private static final TupleFactory _TUPLE_FACTORY = TupleFactory.getInstance();
    private static final BagFactory _BAG_FACTORY = BagFactory.getInstance();

    // required for tests 
    // when absent, tests fail with InstantiationException
    // by logic there should be either p or k and n parameters
    public UniformRandomSample() {}

    protected static double p = 0d; // a fraction of the set came as input parameter
    public UniformRandomSample(String ps) {
        this.p = Double.parseDouble(ps);
        if (this.p > 1) {
            throw new RuntimeException("p should not exceed 1.0");
        }
    }

    public UniformRandomSample(double p) {
        // not sure how to implement the following: 
    	// on p = 0, no further calcs are needed, return empty set
    	// on p = 1, return the input
    	this.p = p;
    }

    protected static long k; // an exact expected number came as input parameter  
    public UniformRandomSample(long k, long total_size) {
    	// on k = input.size, return the input, no further calcs
    	this.k = k;
        this.p = (double) k / total_size;
    }

    @Override
    public String getInitial() {
    	return Initial.class.getName();
    }

    @Override
    public String getIntermed() {
    	return Intermed.class.getName();
    }

    @Override
    public String getFinal() {
    	return Final.class.getName();
    }

    @Override
    public Schema outputSchema(Schema input) {
    	try {
            Schema.FieldSchema inputFieldSchema = input.getField(0);

            if (inputFieldSchema.type != DataType.BAG) {
                throw new RuntimeException("Expected a BAG as input");
            }

            return new Schema(new Schema.FieldSchema(super.getSchemaName(OUTPUT_BAG_NAME_PREFIX, input),
                          inputFieldSchema.schema, DataType.BAG));
    	} catch (FrontendException e) {
            throw new RuntimeException(e);
    	}
    }

    /**
     * this Initial supposed to be executed in the mapper and each it's thread is
     * independent on others, for cases when k % number_of_threads =/= 0 we will
     * get more items in the final than has been requested
     *
     */

    static public class Initial extends EvalFunc<Tuple> {

        public Initial(){}

    	private static RandomDataImpl _RNG = new RandomDataImpl();
    	private static int nextInt(int n) {
            return _RNG.nextInt(0, n);
    	}

    	@Override
        public Tuple exec(Tuple input) throws IOException {
            int numArgs = input.size();
            int k_local;

            if (numArgs != 2) {
                throw new IllegalArgumentException("The input tuple should have two fields: "
                    + "a bag of items and the sampling.");
            }

            DataBag items = (DataBag) input.get(0);
            // the set should not exceed int, if initial set is bigger than max_int,
            // split into sub-sets
            if (items.size() > 2147483647){
                throw new IndexOutOfBoundsException("bag size is above int maximum");
            }
            int numItems = (int) items.size();
            int in = (int) input.get(1);
            if (in > 1){
                // use global p, it doesn't change anyway 
                k_local = (int) Math.ceil(p * numItems);
            } else {
                k_local = (int) Math.ceil(in * numItems);
            }

            DataBag selected = _BAG_FACTORY.newDefaultBag();

            int x;
            SortedSet<Integer> nums = new TreeSet<Integer>();
            while (nums.size() < k_local){
    	        x = nextInt(numItems);
    	        nums.add(x);
    	    }
            int i=0;
            int j;
            Iterator<Integer> it = nums.iterator();
            Iterator<Tuple> it2 = items.iterator();

    	    while (nums.size() > 0){
    	        for ( j=i; j<it.next(); j++) {
    		    it2.next();
    	        }
    	        selected.add(it2.next());
    	        i=j+1;
    	        nums.remove(it.next());
    	    }
            /*
             * The output tuple contains the following fields:
             * <number of items requested to be in the sample (int),>
             * number of processed items in this tuple (int),
             * a bag of selected items (bag).
             */
            Tuple output = _TUPLE_FACTORY.newTuple();

            //output.append(k_local);
            output.append(numItems);
            output.append(selected);

            return output;
      }
    }

    // this intermediate is just for consistency
    static public class Intermed extends EvalFunc<Tuple> {

        public Intermed() {}

    	@Override
        public Tuple exec(Tuple input) throws IOException {
            return input;
        }
    }

    // this final should be executed as reducer
    // merges all selected bags into the output
    // remove excess in case more than requested items
    // were selected
    static public class Final extends EvalFunc<DataBag> {

        public Final(){}

    	@Override
    	public DataBag exec(Tuple input) throws IOException {
            DataBag bag = (DataBag) input.get(0);

            long n_total = 0L; // number of overall items

            DataBag selected = _BAG_FACTORY.newDefaultBag();

            Iterator<Tuple> it = bag.iterator();
            Tuple tuple = it.next();
            while(it.hasNext()) {
                n_total += (long) tuple.get(0);
                selected.addAll((DataBag) tuple.get(1));
                tuple = it.next();
            }
            n_total += (long) tuple.get(0);
            DataBag lastBag = (DataBag) tuple.get(1);

    	    // k || p come from the parent
            long s; // final requested sample size
            if (p > 0){
                s = (long) Math.ceil(p * n_total);
            } else {
                s = k;
            }

            it = lastBag.iterator();
            while(it.hasNext() && selected.size() < s ) {
    	        selected.add(it.next());
            }

            return selected;
    	}
    }

}
