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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Generates a count of the number of distinct tuples in a bag, up to a preset limit.
 *
 * If the number of distinct tuples is small, performance is comparable to using Pig's DISTINCT and COUNT in a nested foreach.
 *
 * Use this UDF when your threshold is low, and some records have a distinct count that is much higher. In such cases this UDF will prevent memory problems
 * and perform an order of magnitude faster than using pure Pig.
 *
 * Example:
 * <pre>
 * {@code
 * DEFINE CountDistinctUpTo10 datafu.pig.bags.CountDistinctUpTo('10');
 * DEFINE CountDistinctUpTo3 datafu.pig.bags.CountDistinctUpTo('3');
 *
 * -- input:
 * -- {(A),(B),(D),(A),(C),(E),(A),(B),(A),(B)}
 * input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY)});
 *
 * -- output:
 * -- (5)
 * output = FOREACH input GENERATE CountDistinctUpTo10(B);
 *
 * -- output2:
 * -- (3)
 * output2 = FOREACH input GENERATE CountDistinctUpTo3(B);
 * }
 * </pre>
 */
public class CountDistinctUpTo extends AccumulatorEvalFunc<Integer> implements Algebraic{

	private static final TupleFactory tupleFactory = TupleFactory.getInstance();
	private static final BagFactory bagFactory = BagFactory.getInstance();

	// for accumulator implementation
	private Set<Tuple> set;
	private final int max;

	public CountDistinctUpTo(String maxAmount) {
		max = Integer.valueOf(maxAmount);
		set = new HashSet<Tuple>(max);
	}

	@Override
	public void accumulate(Tuple tuple) throws IOException {
		count(set, tuple, max, log);
	}

	/**
	 * Counts tuples with a single level of embedding (input contains a bag containing tuples with the original schema)
	 *
	 * Used by both accumulator and algebraic implementations
	 */
	private static void count(Set<Tuple> set, Tuple input, int max, Log log) throws ExecException {
		if (set.size() == max) {
			return; // don't bother looking at the rest of the input
		}

		if (input == null) {
			return;
		}

		DataBag bag = (DataBag) input.get(0);

		if (bag == null) {
			return;
		}

		for (Tuple t : bag) {
			if (set.add(t) && (set.size() == max)) {
				return;
			}
		}
	}

	/**
	 * Counts tuples with two levels of embedding (input contains a bag containing bags of tuples with the original schema)
	 *
	 * Used by the algebraic implementation. Returns null if the maximum was reached
	 */
	private static Set<Tuple> makeDistinctSet(Tuple input, int max, Log log) throws ExecException {
		Set<Tuple> set = new HashSet<Tuple>(max);

		DataBag bag = (DataBag) input.get(0);

		for (Tuple t : bag) {

			// we've encountered a null value from the combiner, therefore the max has already been reached (obviously, not used in the combiner itself)
			if (t.get(0) == null) {
				return null;
			}

			count(set, t, max, log);

			// we've just now reached the maximum
			if (set.size() == max) {
				return null;
			}
		}

		return set;
	}

	@Override
	public void cleanup() {
		set.clear();
	}

	@Override
	public Integer getValue() {
		return set.size();
	}

	@Override
	public String getInitial() {
		return Initial.class.getName();
	}

	@Override
	public String getIntermed() {
		return Intermediate.class.getName();
	}

	@Override
	public String getFinal() {
		return Final.class.getName();
	}

	/**
	 * Outputs a tuple containing a DataBag containing a single tuple T (the original schema) or an empty bag
	 *
	 * <pre>
     * {@code
	 *  T -> ({T})
	 * }
	 * </pre>
	 */
	public static class Initial extends EvalFunc<Tuple> {

		public Initial() {}
		public Initial(String maxAmount) {}

		@Override
		public Tuple exec(Tuple input) throws IOException {
			DataBag inputBag = (DataBag) input.get(0);
			Iterator<Tuple> it = inputBag.iterator();
			DataBag outputBag = bagFactory.newDefaultBag();

			if (it.hasNext()) {
				Tuple t = (Tuple) it.next();
				if ((t != null) && (t.size() > 0) && (t.get(0) != null)) {
					outputBag.add(t);
				}
			}

			return tupleFactory.newTuple(outputBag);
		}
	}

	/**
	 * Receives a bag of bags, each containing a single tuple with the original input schema T
	 *
	 * Outputs a bag of distinct tuples each with the original schema T:

	 * <pre>
     * {@code
	 * {({T}),({T}),({T})} -> ({T, T, T})
	 * }
	 * </pre>
	 *
	 * or if the maximum is reached, null:
	 *
	 * <pre>
     * {@code
	 * {({T}),({T}),({T}) ..} -> (null)
	 * }
	 * </pre>
	 */
	public static class Intermediate extends EvalFunc<Tuple> {

		private final int max;

		public Intermediate() {
			this("0");
		}

		public Intermediate(String maxAmount) {
			max = Integer.valueOf(maxAmount);
		}

		@Override
		public Tuple exec(Tuple input) throws IOException {
			Set<Tuple> set = makeDistinctSet(input, max, log);

			// this is the optimistic case, in which we already have enough distinct tuples in the combiner to stop counting
			if (set == null){
				Tuple result = tupleFactory.newTuple(1);
				result.set(0, null);
				return result;
			}

			DataBag outputBag = bagFactory.newDefaultBag();

			for (Tuple t : set) {
				outputBag.add(t);
			}

			return tupleFactory.newTuple(outputBag);
		}
	}

	/**
	 * Receives output either from initial results or intermediate
	 *
	 * Outputs an integer with the number of distinct tuples, up to the maximum desired.
	 *
	 * <pre>
     * {@code
	 * {({T}),({T,T,T})} -> (3)
	 *
	 * or
	 *
	 * {({T}),({T,T,T}),(null)} -> (MAX)
	 * }
	 * </pre>
	 */
	public static class Final extends EvalFunc<Integer> {

		private final int max;

		public Final() {
			this("0");
		}

		public Final(String maxAmount) {
			max = Integer.valueOf(maxAmount);
		}

		@Override
		public Integer exec(Tuple input) throws IOException {
			Set<Tuple> set = makeDistinctSet(input, max, log);

			if (set == null) {
				return max;
			}

			return set.size();
		}
	}

	@Override
	public Schema outputSchema(Schema input) {

		if (input.size() != 1) {
			throw new RuntimeException("Expected a single field of type bag, but found " + input.size() + " fields");
		}

		FieldSchema field;
		try {
			field = input.getField(0);

			if (field.type != DataType.BAG) {
				throw new RuntimeException("Expected a bag but got: " + DataType.findTypeName(field.type));
			}
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}

		return new Schema(new FieldSchema("CountDistinctUpTo", DataType.INTEGER));
	}
}
