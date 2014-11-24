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

package datafu.pig.hash.lsh.metric;

import java.io.IOException;

import org.apache.commons.math.linear.RealVector;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import datafu.pig.hash.lsh.util.DataTypeUtil;

/**
 * A base UDF used to find a vector v in a bag such that for query point q, metric m and threshold t
 * m(v,q) &lt; t.  In other words, find the first vector in the bag within a threshold distance away.
 *
 * <p>
 * It returns one of the tuples of the bag of vectors.  For an example of its use, please see datafu.pig.hash.lsh.CosineDistanceHash.
 * </p>
 *
 * @see datafu.pig.hash.lsh.CosineDistanceHash
 */
public abstract class MetricUDF extends EvalFunc<Tuple>
{
  protected int dim;
  
  /**
   * Create a new Metric UDF with a given dimension.
   * 
   * @param sDim dimension
   */
  public MetricUDF(String sDim)
  {
    dim = Integer.parseInt(sDim);
  }
  
  /**
   * The distance metric used.  Given v1 and v2, compute the distance between those vectors.
   * @param v1 first vector
   * @param v2 second vector
   * @return the distance between v1 and v2
   */
  protected abstract double dist(RealVector v1, RealVector v2);
  
  /**
   * This UDF expects a query vector as the first element, a threshold (double) as the second, and a bag of vectors.
   * Vectors are represented by tuples with doubles as elements or bags of tuples representing position and value
   * in the case of sparse vectors.
   *
   * <p>
   * It returns one of the tuples of the bag of vectors.  For an example of its use, please see datafu.pig.hash.lsh.CosineDistanceHash.
   * </p>
   *
   * @see datafu.pig.hash.lsh.CosineDistanceHash
   */
  @Override
  public Tuple exec(Tuple input) throws IOException {
    Object firstElement = input.get(0);
    double distanceRange = ((Number)input.get(1)).doubleValue();
    DataBag vectorBag = (DataBag)input.get(2);
    RealVector referenceVector = null;
    if(firstElement instanceof Tuple)
    {
      //in which case the first element is a non-sparse tuple
      referenceVector = DataTypeUtil.INSTANCE.convert((Tuple)firstElement, dim);
    }
    else {
      //in which case the first element is a bag, representing a sparse tuple
      referenceVector = DataTypeUtil.INSTANCE.convert(input, dim);
    }
    
    for(Tuple vecTuple : vectorBag )
    {
      Object vectorObj = vecTuple.get(0);
      RealVector v2 = null;
      if(vectorObj instanceof Tuple)
      {
        v2 = DataTypeUtil.INSTANCE.convert((Tuple)vecTuple.get(0), referenceVector.getDimension());
      }
      else
      {
        v2 = DataTypeUtil.INSTANCE.convert(vecTuple, referenceVector.getDimension());
      }
      double dist = dist(referenceVector, v2);
      if(dist < distanceRange)
      {
        return vecTuple;
      }
    }
    return null;
  }

  /**
   * Create the output schema, based on the input schema.
   *
   * @return the output schema, which is a tuple matching the schema of the third input field.
   */
   public Schema outputSchema(Schema input) {
          try{
            validateInputSchema(input);
            FieldSchema fieldSchema = input.getField(2);
            return fieldSchema.schema;
          }catch (Exception e) {
                 throw new RuntimeException("Unable to create output schema", e);
          }
   }

   /**
    * Validate the input schema to ensure that our input is consistent and that we fail fast.
    * @param input input schema
    * @throws FrontendException
    */
   private void validateInputSchema(Schema input) throws FrontendException
   {
     {
       FieldSchema vectorSchema = input.getField(0);
       if(!DataTypeUtil.isValidVector(vectorSchema, dim))
       {
         throw new FrontendException("Invalid vector element: Expected either a tuple or a bag, but found " + vectorSchema);
       }
     }

     {
       FieldSchema distanceSchema = input.getField(1);
       if(distanceSchema.type != DataType.DOUBLE
       && distanceSchema.type != DataType.INTEGER
       && distanceSchema.type != DataType.LONG
       )
       {
         throw new FrontendException("Invalid distance element: Expected a number, but found " + distanceSchema);
       }
     }

     {
       FieldSchema pointsSchema = input.getField(2);
       if( pointsSchema.type != DataType.BAG)
       {
         throw new FrontendException("Invalid points element: Expected a bag, but found " + pointsSchema);
       }
       FieldSchema tupleInBag = pointsSchema.schema.getField(0);
       FieldSchema vectorInTuple = tupleInBag.schema.getField(0);
       if(!DataTypeUtil.isValidVector(vectorInTuple, dim))
       {
         throw new FrontendException("Invalid points element: Expected a bag of vectors, but found " + vectorInTuple.schema);
       }
     }
   }
}
