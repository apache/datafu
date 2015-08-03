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
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * <p>
 * This UDF will extract a tuple from a bag based on a specified index.
 * </p>
 * <p>
 * There are three input parameter:
 * </p>
 * <ol>
 * <li>DataBag</li>
 * <li>Index</li>
 * <li>Default tuple (Optional)</li>
 * </ol>
 * <p>
 * Example:
 * </p>
 * <pre>
 * {@code
 * define TupleFromBag datafu.pig.bags.TupleFromBag();
 * %declare defaultTuple TOTUPLE(0,'NO_NUMBER')
 *
 * data = LOAD 'input' using PigStorage(',') AS (a:INT,b:CHARARRAY);
 * -- input:
 * (1,a)
 * (1,b)
 * (1,c)
 * (2,d)
 * (2,e)
 * (2,f)
 * (3,g)
 * (3,h)
 * (3,i)
 *
 * grouped = GROUP data BY a;
 *
 * --output:
 * {group: int,data: {(a: int,b: chararray)}}
 * (1,{(1,c),(1,b),(1,a)})
 * (2,{(2,f),(2,e),(2,d)})
 * (3,{(3,i),(3,h),(3,g)})
 *
 * result1 = FOREACH grouped GENERATE
 *           group AS a,
 *           TupleFromBag(data, 0);
 *
 * --output:
 * {a: int,(a: int,b: chararray)}
 * (1,(1,c))
 * (2,(2,f))
 * (3,(3,i))
 *
 *
 * result2 = FOREACH grouped GENERATE
 *           group AS a,
 *           TupleFromBag(data,0).b as first_b,
 *           TupleFromBag(data,1).b as second_b;
 *
 * --output:
 * {a: int,first_b: chararray,second_b: chararray}
 * (1,c,b)
 * (2,f,e)
 * (3,i,h)
 *
 *
 * result3 = FOREACH grouped GENERATE
 *           group AS a,
 *           TupleFromBag(data,0).b as first_b,
 *           TupleFromBag(data,3).b as forth_b;
 * 
 * --output:
 * {a: int,first_b: chararray,forth_b: chararray}
 * (1,c,)
 * (2,f,)
 * (3,i,)
 *
 * result4 = FOREACH grouped GENERATE
 *           group AS a,
 *           TupleFromBag(data,0,$emptyTuple).b as first_b,
 *           TupleFromBag(data,3,$emptyTuple).b as forth_b;
 * 
 * --output:
 * {a: int,first_b: chararray,forth_b: chararray}
 * (1,c,NO_NUMBER)
 * (2,f,NO_NUMBER)
 * (3,i,NO_NUMBER)
 * } 
 * </pre>
 */

public class TupleFromBag extends EvalFunc<Tuple>{

	@Override
	public Tuple exec(Tuple tinput) throws IOException
	{

		try{
			DataBag samples = (DataBag) tinput.get(0);

			int tupleIndex = 0;
			int index = ((Number)tinput.get(1)).intValue();
			for (Tuple tuple : samples) {
				if(tupleIndex == index){
					return tuple;
				}
				tupleIndex++;
			}
		}
		catch (Exception e){
			return null;
		}
		if (tinput.size() == 3){
			return DataType.toTuple(tinput.get(2));
		}

		return null;
	}

	@Override
	public Schema outputSchema(Schema input)
	{
		try {
			if (!(input.size() == 2 || input.size() == 3))
			{
				throw new RuntimeException("Expected input to have two or three fields");
			}

			if (input.getField(1).type != DataType.INTEGER ) {
				throw new RuntimeException("Expected an INT as second input, got: "+input.getField(1).type);
			}

			return new Schema(input.getField(0).schema);
		}

		catch (FrontendException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
