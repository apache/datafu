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

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Get distinct elements in a bag by a given set of field positions.
 * The input and output schemas will be identical.  
 * 
 * The first tuple containing each distinct combination of these fields will be taken.
 * 
 * This operation is order preserving.  If both A and B appear in the output,
 * and A appears before B in the input, then A will appear before B in the output.
 * 
 * Example:
 * <pre>
 * {@code
 * define DistinctBy datafu.pig.bags.DistinctBy('0');
 * 
 * -- input:
 * -- ({(a, 1),(a,1),(b, 2),(b,22),(c, 3),(d, 4)})
 * input = LOAD 'input' AS (B: bag {T: tuple(alpha:CHARARRAY, numeric:INT)});
 * 
 * output = FOREACH input GENERATE DistinctBy(B);
 * 
 * -- output:
 * -- ({(a,1),(b,2),(c,3),(d,4)})
 * } 
 * </pre>
 */
public class DistinctBy extends AccumulatorEvalFunc<DataBag>
{
  private HashSet<Integer> fields = new HashSet<Integer>();
  private HashSet<Tuple> seen = new HashSet<Tuple>();
  private DataBag outputBag;
  
  public DistinctBy(String... fields)
  {
    for(String field : fields) {
      this.fields.add(Integer.parseInt(field));
    }
    cleanup();
  }

  @Override
  public void accumulate(Tuple input) throws IOException
  {
    if (input.size() != 1) {
      throw new RuntimeException("Expected input to have only a single field");
    }    
    if (input.getType(0) != DataType.BAG) {
      throw new RuntimeException("Expected a BAG as input");
    }
    
    DataBag inputBag = (DataBag)input.get(0);
    for (Tuple t : inputBag) {
      Tuple distinctFieldTuple = getDistinctFieldTuple(t, this.fields);
      if (!seen.contains(distinctFieldTuple)) {
        outputBag.add(t);
        seen.add(distinctFieldTuple);
      }
    }
  }

  @Override
  public void cleanup()
  {
    seen.clear();
    outputBag = BagFactory.getInstance().newDefaultBag();
  }

  @Override
  public DataBag getValue()
  {
    return outputBag;
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
      
      Schema inputBagSchema = inputFieldSchema.schema;

      if (inputBagSchema.getField(0).type != DataType.TUPLE)
      {
        throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                                                 DataType.findTypeName(inputBagSchema.getField(0).type)));
      }
      
      Schema inputTupleSchema = inputBagSchema.getField(0).schema;
      
      Schema outputTupleSchema = inputTupleSchema.clone();     
      
      return new Schema(new Schema.FieldSchema(
            getSchemaName(this.getClass().getName().toLowerCase(), input),
            outputTupleSchema, 
            DataType.BAG));
    }
    catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    catch (FrontendException e) {
      throw new RuntimeException(e);
    }
  }
  
  private Tuple getDistinctFieldTuple(Tuple t, HashSet<Integer> distinctFieldPositions) throws ExecException {
    Tuple fieldTuple = TupleFactory.getInstance().newTuple(distinctFieldPositions.size());
    int idx = 0;
    for(int i=0; i<t.size(); i++) {
      if (distinctFieldPositions.contains(i)) {
        fieldTuple.set(idx, t.get(i));
        idx++;
      }
    }
    return fieldTuple;
  }

}
