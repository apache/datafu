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
 * A locality sensitive hash associated with the L1 metric.  This uses a 1-stable distribution
 * to construct the hash.
 *
 */
public class L1LSH extends AbstractStableDistributionFunction implements Sampler
{
  /**
   * Constructs a new instance.
   *
   * @param dim The dimension of the vectors to be hashed
   * @param w A double representing the quantization parameter (also known as the projection width)
   * @param rand The random generator used
   * @throws MathException MathException
   */
  public L1LSH(int dim, double w, RandomGenerator rand) throws MathException {
    super(dim, w, rand);
  }

  /**
   * Draw a sample s ~ Cauchy(0,1), which is 1-stable.
   *
   * @param randomData random data generator
   * @return a sample from a cauchy distribution with median 0 and scale 1
   * @throws MathException MathException
   */
  public double sample(RandomDataImpl randomData) throws MathException {
    return randomData.nextCauchy(0, 1);
  }

  @Override
  protected Sampler getSampler() {
    return this;
  }
}
