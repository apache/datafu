/*
 * Copyright 2018 IICOLL
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package datafu.pig.sampling;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

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

    protected static Tuple getPNK(String pi){
        Tuple pnk = _TUPLE_FACTORY.newTuple();
        long n, k;
        double p;
        String[] ss = pi.split(",");
        if (pi.startsWith("0.") || pi.startsWith(".")){
            try {
                p = Double.parseDouble(ss[0].trim());
                pnk.append(p);
            } catch (NumberFormatException e) {
                throw new RuntimeException("p should be a number, got NumberFormatException:"+pi);
            }
        } else {
            if (ss.length == 2){
                try {
                    k = Long.parseLong(ss[0].trim(), 10);
                    n = Long.parseLong(ss[1].trim(), 10);
                    p = (double) k / n;
                    pnk.append(p);
                    pnk.append(k);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("k and n should be numbers, got NumberFormatException:"+pi);
                }
            } else {
                throw new RuntimeException("2 parameters are required k and n, got:"+pi);
            }
        }
        return pnk;
    }

    /**
     * 1st mapped data processing step, can't be skipped
     *
     */
    static public class Initial extends EvalFunc<Tuple> {

        public Initial() {}

        private Tuple pnk;

        public Initial(String pi){
            pnk = getPNK(pi);
        }

    	private static RandomDataImpl _RNG = new RandomDataImpl();
    	synchronized private static int nextInt(int n) {
            return _RNG.nextInt(0, n);
    	}

    	@Override
        public Tuple exec(Tuple input) throws IOException {
            DataBag items = (DataBag) input.get(0);

            DataBag selected = _BAG_FACTORY.newDefaultBag();
            Tuple extra = _TUPLE_FACTORY.newTuple();
            Tuple tu;
            Tuple output = _TUPLE_FACTORY.newTuple();
            Double p = (Double) pnk.get(0);

            if (items.size() == 0 || p == 0d) {
                return _TUPLE_FACTORY.newTuple();
            } else if (items.size() == 1) {
                tu = items.iterator().next();
                extra = tu;
                output.append(1);
                output.append(selected);
                output.append(extra);

                return output;
            }

            // the set should not exceed int, if initial set is bigger than max_int,
            // split into sub-sets
            if (items.size() > Integer.MAX_VALUE){
                throw new IndexOutOfBoundsException("bag size is above int maximum");
            }
            int numItems = (int) items.size();

            int k_down = (int) Math.floor(p * numItems);
            int k_up = (int) Math.ceil(p * numItems);
            if (k_up == 0) return _TUPLE_FACTORY.newTuple();

            int x;
            numItems--;
            SortedSet<Integer> nums = new TreeSet<Integer>();
            int kk = k_up;

            // if we need to return more than a half of input elements
            // insteaed of addition it make sense to make exclusion
            // I mean
            // p <= 0.5
            // add selected randomly elements to output set
            // p > 0.5
            // add all elements except randomly selected
            // for exclusion if we need an extra element
            // take the 1st, since at the end eventually no
            // elements to take from have left

            if (p <= 0.5){
                k_down = numItems - k_down + 1;
                k_up = numItems - k_up + 1;
                kk = k_down;
            }
            while (nums.size() < kk){
    	        x = nextInt(numItems);
    	        nums.add(x);
    	    }

            int i=0;
            int j;
            Iterator<Integer> it = nums.iterator();
            Iterator<Tuple> it2 = items.iterator();
            tu = it2.next();
            int ii = it.next();

            if (p > 0.5){
                while(i == ii){
                    i++;
                    ii = it.next();
    	            tu = it2.next();
                }
                // add the 1st valid element
                if (k_down == k_up){
                    selected.add(tu);
                } else {
                    extra=tu;
                }
                tu = it2.next();
                i++;
            }

    	    while (it.hasNext()){
                if (p <= 0.5){
    	            for ( j=i; j<ii; j++) {
    		        tu = it2.next();
    	            }
    	            selected.add(tu);
                } else {
    	            for ( j=i; j<ii; j++) {
       	                selected.add(tu);
    		        tu = it2.next();
    	            }
                }
                i=j+1;
                ii = it.next();
    	        tu = it2.next();
    	    }
            if (p <= 0.5){
                for ( j=i; j<ii; j++) {
                    tu = it2.next();
                }
                if (k_down == k_up){
                    selected.add(tu);
                } else {
                    extra=tu;
                }
            } else {
                for ( j=i; j<ii; j++) {
                    selected.add(tu);
                    tu = it2.next();
                }
                if (it2.hasNext()){
                    tu = it2.next();
                }
                while (it2.hasNext()) {
                    selected.add(tu);
                    tu = it2.next();
                }
            }
            /*
             * The output tuple contains the following fields:
             * number of processed items in this tuple (int),
             * a bag of selected items (bag),
             * an extra tuple from the original set for cases
             * when the exact number/fraction can't be obtained
             * (tuple).
             */

            output.append(numItems+1);
            output.append(selected);
            output.append(extra);

            return output;
      }
    }

    /**
     * this intermediate is for initial selection join into bigger chunks
     */
    static public class Intermed extends EvalFunc<Tuple> {

        public Intermed(){}

        private Tuple pnk;

        public Intermed(String pi){
            pnk = getPNK(pi);
        }

    	@Override
        public Tuple exec(Tuple input) throws IOException {
            DataBag bag = (DataBag) input.get(0);
            DataBag selected = _BAG_FACTORY.newDefaultBag();
            DataBag in_extra = _BAG_FACTORY.newDefaultBag();
            Tuple out_extra = _TUPLE_FACTORY.newTuple();
            long numItems = 0L;
            long gotItems = 0L;
            long required;

            for (Tuple tuple : bag){
                numItems += ((Number) tuple.get(0)).longValue();
                gotItems += ((Number)((DataBag) tuple.get(1)).size()).longValue();
                selected.addAll((DataBag) tuple.get(1));
                in_extra.add((Tuple) tuple.get(2));
            }

            Double p = (Double) pnk.get(0);
            long required_up = (long) Math.ceil(p * numItems);
            long required_down = (long) Math.floor(p * numItems);

            if (in_extra.size() > 0) {
                Iterator<Tuple> it = in_extra.iterator();
                Tuple tu = it.next();
                if (in_extra.size() == 1 && gotItems < required_down) {
                    selected.add(tu);
                } else {
                    while (gotItems < required_down && it.hasNext()){
                        selected.add(tu);
                        gotItems++;
                        tu = it.next();
                    }
                    if (tu != null && required_down < required_up) { out_extra = tu; }
                }
            }

            Tuple output = _TUPLE_FACTORY.newTuple();
            output.append(numItems);
            output.append(selected);
            output.append(out_extra);
            return output;
        }
    }


    /**
     * this final should be executed as reducer
     * merges all selected bags into the output
     * adding extra in case more elements needed
     */
    static public class Final extends EvalFunc<DataBag> {

        public Final(){}

        private Tuple pnk;

        public Final(String pi){
            pnk = getPNK(pi);
        }

    	@Override
    	public DataBag exec(Tuple input) throws IOException {
            DataBag bag = (DataBag) input.get(0);

            if (bag.size() == 0) { return _BAG_FACTORY.newDefaultBag(); }

            long n_total = 0L;

            DataBag selected = _BAG_FACTORY.newDefaultBag();
            DataBag extra = _BAG_FACTORY.newDefaultBag();

            Iterator<Tuple> it = bag.iterator();
            Tuple tuple = it.next();
            while(it.hasNext()) {
                n_total += ((Number) tuple.get(0)).longValue();
                selected.addAll((DataBag) tuple.get(1));
                extra.add((Tuple) tuple.get(2));
                tuple = it.next();
            }
            n_total += ((Number) tuple.get(0)).longValue();
            selected.addAll((DataBag) tuple.get(1));
            extra.add((Tuple) tuple.get(2));

            long s; // final requested sample size
            if (pnk.size() > 1){
                s = ((Number) pnk.get(1)).longValue();
            } else {
                Double p = (Double) pnk.get(0);
                s = (long) Math.ceil(p * n_total);
            }

            it = extra.iterator();
            while(it.hasNext() && selected.size() < s ) {
    	        selected.add(it.next());
            }

            return selected;
    	}
    }

}
