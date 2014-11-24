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

import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.random.RandomGenerator;

/**
 * An abstract class representing a locality sensitive hash. From wikipedia's article on <a href="http://en.wikipedia.org/wiki/Locality-sensitive_hashing" target="_blank">Locality Sensitive Hashing</a>:
 *
 * <p>
 * Locality-sensitive hashing (LSH) is a method of performing probabilistic dimension reduction of high-dimensional data.
 * The basic idea is to hash the input items so that similar items are mapped to the same buckets with high probability
 * (the number of buckets being much smaller than the universe of possible input items).
 * </p>
 */
public abstract class LSH
{
  protected RandomGenerator rg;
  protected int dim;

  /**
   * Construct a locality sensitive hash.  Note, one may pass a pre-seeded generator.
   *
   * @param dim The dimension of the vectors which are to be hashed
   * @param rg The random generator to use internally.
   */
  public LSH(int dim, RandomGenerator rg)
  {
    this.dim = dim;
    this.rg = rg;
  }

  /**
   *
   * @return The random generator from this LSH
   */
  public RandomGenerator getRandomGenerator() { return rg;}
  /**
   *
   * @return The dimension of the vectors which this LSh supports
   */
  public int getDim() { return dim; }

  /**
   * Hash a vector.
   *
   * @param vector A vector to be hashed
   * @return A hash which collides with vectors close according to some metric (implementation dependent).
   */
  public abstract long apply(RealVector vector);

}
