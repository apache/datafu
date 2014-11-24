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

package datafu.pig.hash.lsh.p_stable;

import org.apache.commons.math.MathException;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.commons.math.random.RandomGenerator;

import datafu.pig.hash.lsh.interfaces.Sampler;

/**
 * A locality sensitive hash associated with the L2 metric.  This uses a 2-stable distribution
 * to construct the hash.
 */
public class L2LSH extends AbstractStableDistributionFunction implements Sampler {

  /**
   * Constructs a new instance.
   * @param dim the dimension of the vectors to be hashed
   * @param w a double representing the quantization parameter (also known as the projection width)
   * @param rand the random generator
   * @throws MathException MathException
   */
  public L2LSH(int dim, double w, RandomGenerator rand) throws MathException {
    super(dim, w, rand);
  }

  /**
   * Draw a sample s ~ Gaussian(0,1), which is 2-stable.
   *
   * @param randomData random data generator
   * @return a sample from a Gaussian distribution with mu of 0 and sigma of 1
   */
   public double sample(RandomDataImpl randomData)
     {
           return randomData.nextGaussian(0,1); 
     }

  @Override
  protected Sampler getSampler() {
    return this;
  }


}
