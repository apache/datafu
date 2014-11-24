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

import java.util.List;

import org.apache.commons.math.MathException;
import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.commons.math.random.RandomGenerator;

import datafu.pig.hash.lsh.interfaces.LSH;

/**
 * A composite hash which takes multiple hashes and composes them.  This is useful
 * when a locality sensitive hash has a range that is very narrow and would want a wider
 * range for the hash.
 * 
 * 
 */
public class RepeatingLSH extends LSH
{
  private List<LSH> lshList;
  private RealVector randomVec;
  
  public RepeatingLSH(List<LSH> lshList) throws MathException
  {
    super(lshList.get(0).getDim(), lshList.get(0).getRandomGenerator());
    this.lshList = lshList;
    RandomGenerator rg = lshList.get(0).getRandomGenerator();
    RandomData rd = new RandomDataImpl(rg);
    /*
     * Compute a random vector of lshList.size() with each component taken from U(0,10)
     */
    randomVec = new ArrayRealVector(lshList.size());
    for(int i = 0; i < randomVec.getDimension();++i)
    {
      randomVec.setEntry(i, rd.nextUniform(0, 10.0));
    }
  }
  

  /**
   * Compute the aggregated hash of a vector.  This is done by taking the output of the k hashes as a
   * vector and computing the dot product with a vector of uniformly distributed components between 0 and 10.
   * 
   * So, consider a_{0,k} such that a_i ~ U(0,10) and hash functions h_{0,k} treated as 2 k-dimensional vectors,
   * we return the dot product of h(v) and a.
   * 
   */
  public long apply(RealVector vector) {
    long res = 0;
    
    for(int i = 0;i < lshList.size();++i)
    {
      res += randomVec.getEntry(i)* lshList.get(i).apply(vector);
    }
    return res;
  }

}
