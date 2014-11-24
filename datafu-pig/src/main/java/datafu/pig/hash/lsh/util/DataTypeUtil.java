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

package datafu.pig.hash.lsh.util;

import java.util.List;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.OpenMapRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.pig.PigException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * A utility function to translate between pig types and vectors.
 */
public enum DataTypeUtil {
  INSTANCE;
  /**
   * Convert a tuple t into a RealVector of dimension dim.
   * The tuple can be of a couple of forms:
   *
   * <ul>
   * <li>A tuple composed of dim numeric types a la (1.0,2.0,3,5.0)</li>
   * <li>A tuple which contains as its first element a tuple like above a la ( (1.0,2.0,3,5.0), 5) ) would yield (1.0,2.0,3,5.0)</li>
   * <li>A bag containing tuples where the first element is the position and the second element is the value.  This is for sparse vectors and it looks like this ( { (0,1.0), (1, 2.0), (3,3), (4,5.0) } ).</li>
   * </ul>
   *
   * @param t The tuple to convert to a vector
   * @param dim The dimension of the vector
   * @return The actual RealVector (which may or may not be sparse)
   * @throws PigException PigException
   */
  public RealVector convert(Tuple t, int dim) throws PigException
  {
    Object firstElement = t.get(0);
    if(firstElement instanceof DataBag)
    {
      return convertBag( (DataBag)firstElement, dim);
    }
    else if(firstElement instanceof Number)
    {
      return convertTuple(t, dim);
    }
    else if(firstElement instanceof Tuple)
    {
      return convert((Tuple)firstElement, dim);
    }
    throw new PigException("Unable to convert tuple into a RealVector.  "
        + "I expected either a tuple of numeric types or a Bag of tuples"
        + " with positions and values.");
  }
  
  private RealVector convertTuple(Tuple t, int dim) throws PigException
  {
    double[] values = new double[dim];
    for(int i = 0;i < t.size();++i)
    {
      values[i] = ((Number)t.get(i)).doubleValue();
    }
    return new ArrayRealVector(values);
  }
  
  private RealVector convertBag(DataBag bag, int dim) throws PigException
  {
    OpenMapRealVector ret = new OpenMapRealVector(dim);
    for(Tuple t : bag)
    {
      if(t.size() != 2)
      {
        throw new PigException("Unable to convert tuple inside bag into a sparse vector."
            + "  Expected tuples of size at least 2 of form (int, java.lang.Number)");
      }
      Integer position = (Integer)t.get(0);
      double value = ((Number)t.get(1)).doubleValue();
      ret.setEntry(position, value);
    }
    return ret;
  }
  private static boolean isValidDenseVector(FieldSchema vectorSchema, int dimension)
  {
    List<FieldSchema> innerFields = vectorSchema.schema.getFields();
    if(innerFields.size() != dimension)
    {
      return false;
    }
    for(FieldSchema innerField : innerFields)
    {
      //check to see if the elements are numeric
      if(innerField.type != DataType.INTEGER 
      && innerField.type != DataType.DOUBLE
      && innerField.type != DataType.LONG
      && innerField.type != DataType.FLOAT
        )
      {
        return false;
      }
    }
    return true;
  }
  
  private static boolean isValidSparseVector(FieldSchema vectorSchema)
  {
    //validate that the bag contains pairs
    FieldSchema tupleSchema = vectorSchema.schema.getFields().get(0);
    if(tupleSchema.type != DataType.TUPLE)
    {
      //a bag of tuples is what I expect here
      return false;
    }
    List<FieldSchema> innerFields = tupleSchema.schema.getFields();
    if(innerFields.size() != 2)
    {
      return false;
    }
    FieldSchema indexField = innerFields.get(0);
    if(indexField.type != DataType.LONG && indexField.type != DataType.INTEGER)
    {
      //it's not a valid index because it's not integral
      return false;
    }
    FieldSchema valueField = innerFields.get(1);
    if(valueField.type != DataType.DOUBLE && valueField.type != DataType.FLOAT)
    {
      //it's not a valid real (floating point) number
      return false;
    }
    return true;
  }
  
  public static boolean isValidVector(FieldSchema inputSchema, int dimension)
  {
    FieldSchema vectorSchema = inputSchema;
    if(vectorSchema.type == DataType.TUPLE)
    {
      //find the inner most tuple, iterating down the first element's
      List<FieldSchema> children = vectorSchema.schema.getFields();
      while(children.size() > 0 && children.get(0).type == DataType.TUPLE)
      {
        vectorSchema = children.get(0);
        children = vectorSchema.schema.getFields();
      }
    }
    if(vectorSchema.type == DataType.BAG)
    {
      return isValidSparseVector(vectorSchema);
    }
    else if(vectorSchema.type == DataType.TUPLE)
    {
      return isValidDenseVector(vectorSchema, dimension);
    }
    else
    {
      //it's neither a bag nor a tuple, so it can't be a vector
      return false;
    }
    
  }
}
