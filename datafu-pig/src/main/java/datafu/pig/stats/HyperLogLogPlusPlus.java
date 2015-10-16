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

package datafu.pig.stats;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import com.clearspring.analytics.hash.MurmurHash;
import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * A UDF that applies the HyperLogLog++ cardinality estimation algorithm.
 *
 * <p>
 * This uses the implementation of HyperLogLog++ from <a href="https://github.com/addthis/stream-lib" target="_blank">stream-lib</a>.
 * The HyperLogLog++ algorithm is an enhanced version of HyperLogLog as described in
 * <a href="http://static.googleusercontent.com/external_content/untrusted_dlcp/research.google.com/en/us/pubs/archive/40671.pdf">here</a>.
 * </p>
 *
 * <p>
 * This is a streaming implementation, and therefore the input data does not need to be sorted.
 * </p>
 *
 */
public class HyperLogLogPlusPlus extends AlgebraicEvalFunc<Long>
{
  private static TupleFactory mTupleFactory = TupleFactory.getInstance();

  private String p;

  /**
   * Constructs a HyperLogLog++ estimator.
   */
  public HyperLogLogPlusPlus()
  {
    this("20");
  }

  /**
   * Constructs a HyperLogLog++ estimator.
   *
   * @param p precision value
   */
  public HyperLogLogPlusPlus(String p)
  {
    super(p);
    this.p = p;
    cleanup();
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try {
      if (input.size() != 1)
      {
        throw new RuntimeException("Expected input to have only a single field");
      }

      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }

      return new Schema(new Schema.FieldSchema(null, DataType.LONG));
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }

  private String param = null;
  private String getParam()
  {
    if (param == null) {
      if (this.p != null) {
        param = String.format("('%s')", this.p);
      } else {
        param = "";
      }
    }
    return param;
  }

  @Override
  public String getFinal() {
      return Final.class.getName() + getParam();
  }

  @Override
  public String getInitial() {
     return Initial.class.getName() + getParam();
  }

  @Override
  public String getIntermed() {
      return Intermediate.class.getName() + getParam();
  }

  static public class Initial extends EvalFunc<Tuple> {
	public Initial() {};
	public Initial(String p) {};


    @Override
    public Tuple exec(Tuple input) throws IOException {
      // Since Initial is guaranteed to be called
      // only in the map, it will be called with an
      // input of a bag with a single tuple - the
      // count should always be 1 if bag is non empty
      DataBag bag = (DataBag) input.get(0);
      Iterator<Tuple> it = bag.iterator();
      Tuple t = null;
      if (it.hasNext()) {
        t = (Tuple) it.next();
      }
      return mTupleFactory.newTuple((Object) MurmurHash.hash64(t));
    }
  }

  static public class Intermediate extends EvalFunc<Tuple> {
	public Intermediate() {
		this("20");
	};
	private String p;
	public Intermediate(String p) {this.p = p;};

    @Override
    public Tuple exec(Tuple input) throws IOException {
      try {
        DataByteArray data = new DataByteArray(countDisctinct(input, Integer.parseInt(p)).getBytes());
        return mTupleFactory.newTuple(data);
      } catch (ExecException ee) {
        throw ee;
      } catch (Exception e) {
        int errCode = 2106;
        String msg = "Error while computing count in "
            + this.getClass().getSimpleName();
        throw new ExecException(msg, errCode, PigException.BUG, e);
      }
    }
  }

  static public class Final extends EvalFunc<Long> {
	public Final() {
		this("20");
	};
	private String p;
	public Final(String p) {this.p = p;};

    @Override
    public Long exec(Tuple input) throws IOException {
      try {
        return countDisctinct(input, Integer.parseInt(p)).cardinality();
      } catch (Exception ee) {
        int errCode = 2106;
        String msg = "Error while computing count in "
            + this.getClass().getSimpleName();
        throw new ExecException(msg, errCode, PigException.BUG, ee);
      }
    }
  }

  static protected HyperLogLogPlus countDisctinct(Tuple input, int p)
      throws NumberFormatException, IOException {
    HyperLogLogPlus estimator = new HyperLogLogPlus(p);
    DataBag values = (DataBag) input.get(0);
    for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
      Tuple t = it.next();
      Object data = t.get(0);
      if (data instanceof Long) {
        estimator.offerHashed((Long)data);
      } else if (data instanceof DataByteArray) {
        DataByteArray bytes = (DataByteArray) data;
        HyperLogLogPlus newEstimator;
        try {
          newEstimator = HyperLogLogPlus.Builder.build(bytes.get());
          estimator = (HyperLogLogPlus) estimator.merge(newEstimator);
        } catch (IOException e) {
          throw new RuntimeException(e);
        } catch (CardinalityMergeException e) {
          throw new RuntimeException(e);
        }
      }
    }
    return estimator;
  }

}
