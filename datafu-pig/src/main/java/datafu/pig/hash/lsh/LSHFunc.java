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

package datafu.pig.hash.lsh;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.math.MathException;
import org.apache.commons.math.linear.RealVector;
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
import org.apache.pig.impl.util.UDFContext;

import datafu.pig.hash.lsh.interfaces.LSHCreator;
import datafu.pig.hash.lsh.util.DataTypeUtil;

/**
 * The base UDF for locality sensitive hashing.  
 */
public abstract class LSHFunc extends EvalFunc<DataBag>
{
  protected LSHFamily lsh = null;
  protected Long seed;
  private TupleFactory mTupleFactory = TupleFactory.getInstance();
  private BagFactory mBagFactory = BagFactory.getInstance();
  private LSHCreator lshCreator = null;
  protected abstract LSHCreator createLSHCreator();
  protected abstract int getDimension();
  
  public LSHFunc(String sSeed)
  {
    if(sSeed == null)
    {
      seed = null;
    }
    else
    {
      seed = Long.parseLong(sSeed);
    }
  }
    /**
     * The expectation is that the input is a Tuple of numeric data representing a real vector.
   * 
   * @return DataBag containing tuples which have the lsh_id (the hash ID in the family) and the hash
     */
  @Override
  public DataBag exec(Tuple t) throws IOException
  {
    if (lsh == null) {
      try {
        lshCreator = createLSHCreator();
        lsh = lshCreator.constructFamily(lshCreator.createGenerator());
      } catch (MathException e) {
        throw new RuntimeException("Unable to construct LSH!", e);
      }
    }

    RealVector r = null;
    try {
      r = DataTypeUtil.INSTANCE.convert(t, lshCreator.getDim());
    } catch (ExecException e) {
      throw new IllegalStateException("Unable to convert tuple: "
          + t.toString() + " to RealVector");
    }
    DataBag ret = mBagFactory.newDefaultBag();
    
    int idx = 0;
    for(Long hash : lsh.apply(r))
    {
      Tuple out = mTupleFactory.newTuple(2);
      out.set(0, idx++);
      out.set(1,  hash);
      ret.add(out);
    }
    return ret;
  }
  
  protected long getSeed()
  {
    if(seed == null)
    {
      UDFContext context = UDFContext.getUDFContext();
      return Long.parseLong(context.getUDFProperties(this.getClass()).getProperty("seed"));
    }
    else
    {
      return seed;
    }
  }
  
  /**
   * The schema returned here is a bag containing pairs.  
   * The pairs are the lsh_id (or the ID of the hash) and the hash value.
   */
   public Schema outputSchema(Schema input) {
         try {
           validateInputSchema(input);
           long randomSeed = new Random().nextLong();
           UDFContext context = UDFContext.getUDFContext();
           context.getUDFProperties(this.getClass()).setProperty("seed", "" + randomSeed);
           Schema bagSchema = new Schema();
           bagSchema.add(new Schema.FieldSchema("lsh_id", DataType.INTEGER));
           bagSchema.add(new Schema.FieldSchema("hash", DataType.LONG));
           return new Schema(new Schema.FieldSchema("lsh", bagSchema, DataType.BAG));
         }catch (Exception e){
            throw new RuntimeException("Unable to create output schema", e);
         }
   }
   /**
    * Validate the input schema to ensure that our input is consistent and that we fail fast.
    * @param input
    * @throws FrontendException
    */
   private void validateInputSchema(Schema input) throws FrontendException
   {
     FieldSchema vectorSchema = input.getField(0);
     if(!DataTypeUtil.isValidVector(vectorSchema, getDimension()))
     {
       throw new FrontendException("Invalid vector element: Expected either a tuple or a bag, but found " + vectorSchema);
     }
   }
}
