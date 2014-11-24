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

package datafu.pig.hash.lsh.interfaces;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math.MathException;
import org.apache.commons.math.random.JDKRandomGenerator;
import org.apache.commons.math.random.RandomGenerator;

import datafu.pig.hash.lsh.LSHFamily;
import datafu.pig.hash.lsh.RepeatingLSH;

/**
 * Create a Locality sensitive hash.
 * 
 */
public abstract class LSHCreator 
{
  private int dim;
  private int numHashes;
  private int numInternalRepetitions;
  private long seed;
  
  /**
   * Create a LSHCreator
   * 
   * @param dim The dimension of the vectors which the LSH will hash
   * @param numHashes The number of locality sensitive hashes to create
   * @param numInternalRepetitions Each locality sensitive hash is a composite of numInternalRepetitions LSHes (this is done to increase range of the LSH)
   * @param seed The seed to use
   */
  public LSHCreator(int dim, int numHashes, int numInternalRepetitions, long seed)
  {
    this.dim = dim;
    this.numHashes = numHashes;
    this.numInternalRepetitions = numInternalRepetitions;
    this.seed = seed;
  }
  protected abstract LSH constructLSH(RandomGenerator rg) throws MathException;
  public int getDim() { return dim;}
  public RandomGenerator createGenerator() 
  {
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(seed);
    return rg;
  }
  
  /**
   * 
   * @return The number of locality sensitive hashes to create
   */
  public int getNumHashes()
  {
    return numHashes;
  }
  /**
   * Each locality sensitive hash is a composite of numInternalRepetitions LSHes (this is done to increase range of the LSH)
   * @return The number of internal repetitions
   */
  public int getNumInternalRepetitions()
  {
    return numInternalRepetitions;
  }
  
  
  /**
   * 
   * @param rg The random generator to use when constructing the family
   * @return The family of locality sensitive hashes
   * @throws MathException MathException
   */
  public  LSHFamily constructFamily(RandomGenerator rg) throws MathException
    { 
      List<LSH> hashes = new ArrayList<LSH>();
      for(int i = 0;i < getNumHashes();++i)
      {
        LSH lsh = null;
        if(getNumInternalRepetitions() == 1)
        {
          // in the situation of 1, we don't do a composite of 1..we just pass the raw LSH back
          lsh = constructLSH(rg);
        }
        else
        {
          List<LSH> lshFamily = new ArrayList<LSH>();
          for(int j = 0;j < getNumInternalRepetitions();++j)
          {
            lshFamily.add(constructLSH(rg));
          }
          lsh = new RepeatingLSH(lshFamily);
        }
        hashes.add(lsh);
      }
      return new LSHFamily(hashes);
    }
}
