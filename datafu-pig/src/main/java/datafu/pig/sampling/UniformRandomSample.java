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
 * DEFINE URS datafu.pig.sampling.UniformRandomSample('p | k & n');
 *
 * item    = LOAD 'input' AS (x:<type>);
 * sampled = FOREACH (GROUP items ALL) GENERATE FLATTEN(URS(items));
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

    public UniformRandomSample(String ps) {
        super(ps);
        // all parameters calculations should be done in nested classes constructors
        // to allow multiple instantiations through calls like:
        // DEFINE URS1 datafu.pig.sampling.UniformRandomSample('$p');
        // DEFINE URS2 datafu.pig.sampling.UniformRandomSample('$k, $n');
        // data = LOAD 'input' AS (A_id:chararray, B_id:chararray, C:int);
        // sampled = FOREACH (GROUP data ALL) GENERATE URS1(data) as sample_1, URS2(data) AS sample_2;
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

    /*
     * this Initial vectorizes input data, making 1 cal per element,
     * for sampling 1 element processing doesn't make much sense,
     * therefore, lets skip initial, just passing data through it
     * and make actual calc in Intermed
     *
     */

    static public class Initial extends EvalFunc<Tuple> {

        public Initial(){}

        public Initial(String pi){}

    	@Override
        public Tuple exec(Tuple input) throws IOException {
            return input;
        }
    }

    /*
     * this intermediate is where a subset selection is done
     */
    static public class Intermed extends EvalFunc<Tuple> {

        public Intermed() {}

        private double p;
        private long k = 0L;

        public Intermed(String pi){
            if (pi.contains(",")){
                try {
                    String[] ss = pi.split(",");
                    k = Long.parseLong(ss[0].trim(), 10);
                    long ts = Long.parseLong(ss[1].trim(), 10);
                    p = (double) k / ts;
                } catch (NumberFormatException e) {
                    throw new RuntimeException("k and n should be numbers, got NumberFormatException:"+pi);
                }
            } else {
                try {
                    p = Double.parseDouble(pi);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("p should be a number, got NumberFormatException:"+pi);
                }
            }
            if (p > 1.0d) {
                throw new RuntimeException("p should not exceed 1");
            }

        }

    	private static RandomDataImpl _RNG = new RandomDataImpl();
    	private static int nextInt(int n) {
            return _RNG.nextInt(0, n);
    	}

    	@Override
        public Tuple exec(Tuple input) throws IOException {
            DataBag items = (DataBag) input.get(0);

            DataBag selected = _BAG_FACTORY.newDefaultBag();
            Tuple tu;
            Tuple output = _TUPLE_FACTORY.newTuple();

            if (items.size() == 0 || p == 0) {
                return _TUPLE_FACTORY.newTuple();
            } else if (items.size() == 1) {
                tu = items.iterator().next();
                selected.add(tu);
                output.append(1);
                output.append(selected);

                return output;
            } else if ( p == 1.0d){
                selected.addAll(items);
                output.append(items.size());
                output.append(selected);

                return output;
    	    }

            // the set should not exceed int, if initial set is bigger than max_int,
            // split into sub-sets
            if (items.size() > Integer.MAX_VALUE){
                throw new IndexOutOfBoundsException("bag size is above int maximum");
            }
            int numItems = (int) items.size();

            int k_local = (int) Math.ceil(p * numItems);
            if (k_local == 0) return _TUPLE_FACTORY.newTuple();

            int x;
            SortedSet<Integer> nums = new TreeSet<Integer>();
            numItems--;
            while (nums.size() < k_local){
    	        x = nextInt(numItems);
    	        nums.add(x);
    	    }

            int i=0;
            int j;
            Iterator<Integer> it = nums.iterator();
            Iterator<Tuple> it2 = items.iterator();
            tu = it2.next();
            int ii = it.next();
    	    while (it.hasNext()){
    	        for ( j=i; j<ii; j++) {
    		    tu = it2.next();
    	        }
    	        selected.add(tu);
    	        i=j+1;
                ii = it.next();
    		tu = it2.next();
    	    }
            for ( j=i; j<ii; j++) {
                    tu = it2.next();
            }
            selected.add(tu);

            /*
             * The output tuple contains the following fields:
             * <number of items requested to be in the sample (int),>
             * number of processed items in this tuple (int),
             * a bag of selected items (bag).
             */

            output.append(numItems+1);
            output.append(selected);

            return output;
      }
    }

    // this final should be executed as reducer
    // merges all selected bags into the output
    // remove excess in case more than requested items
    // were selected
    static public class Final extends EvalFunc<DataBag> {

        public Final(){}

        private double p;
        private long k = 0L;

        public Final(String pi){

            if (pi.contains(",")){
                try {
                    String[] ss = pi.split(",");
                    k = Long.parseLong(ss[0].trim(), 10);
                    long ts = Long.parseLong(ss[1].trim(), 10);
                    p = (double) k / ts;
                } catch (NumberFormatException e) {
                    throw new RuntimeException("k and n should be numbers, got NumberFormatException:"+pi);
                }
            } else {
                try {
                    p = Double.parseDouble(pi);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("p should be a number, got NumberFormatException:"+pi);
                }
            }
            if (p > 1.0d) {
                throw new RuntimeException("p should not exceed 1");
            }
        }

    	@Override
    	public DataBag exec(Tuple input) throws IOException {
            DataBag bag = (DataBag) input.get(0);

            if (bag.size() == 0) { return _BAG_FACTORY.newDefaultBag(); }

            long n_total = 0L;

            DataBag selected = _BAG_FACTORY.newDefaultBag();

            Iterator<Tuple> it = bag.iterator();
            Tuple tuple = it.next();
            while(it.hasNext()) {
                n_total += ((Number) tuple.get(0)).longValue();
                selected.addAll((DataBag) tuple.get(1));
                tuple = it.next();
            }
            n_total += ((Number) tuple.get(0)).longValue();
            DataBag lastBag = (DataBag) tuple.get(1);

            long s; // final requested sample size
            if (k == 0){
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
