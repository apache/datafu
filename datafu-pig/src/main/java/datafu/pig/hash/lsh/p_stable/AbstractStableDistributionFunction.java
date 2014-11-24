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
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.commons.math.random.RandomGenerator;

import datafu.pig.hash.lsh.interfaces.LSH;
import datafu.pig.hash.lsh.interfaces.Sampler;

/**
 * This is the base-class for all p-stable based locality sensitive hashes. p-stable locality sensitive
 * hashes are defined by a few parameters: a dimension, d , a vector taken from a
 * <a href="http://en.wikipedia.org/wiki/Stable_distribution" target="_blank">k-stable distribution</a>
 * (where k is 1 or 2) and a width of projection, w.
 *
 * <p>
 * All p-stable LSH functions are parameterized with a quantization parameter (w or r in
 * the literature , depending on where you look). Consider the following excerpt
 * from Datar, M.; Immorlica, N.; Indyk, P.; Mirrokni, V.S. (2004).
 * "Locality-Sensitive Hashing Scheme Based on p-Stable Distributions".
 * Proceedings of the Symposium on Computational Geometry.
 * </p>
 *
 * <p>
 * Decreasing the width of the projection (w) decreases the probability of collision for any two points.
 * Thus, it has the same effect as increasing k . As a result, we would like to set w as small as possible
 * and in this way decrease the number of projections we need to make.
 * </p>
 *
 * <p>
 * In the literature, the quantization parameter (or width of the projection) is
 * found empirically given a sample of the data and the likely threshold for
 * the metric. Tuning this parameter is very important for the performance of
 * this algorithm. For more information, see Datar, M.; Immorlica, N.; Indyk,
 * P.; Mirrokni, V.S. (2004).
 * "Locality-Sensitive Hashing Scheme Based on p-Stable Distributions".
 * Proceedings of the Symposium on Computational Geometry.
 * </p>
 *
 */
public abstract class AbstractStableDistributionFunction extends LSH
{
   private double[] a;
   private double b;
   double w;

   /**
    * Constructs a new instance.
    * @param dim The dimension of the vectors to be hashed
    * @param w A double representing the quantization parameter (also known as the projection width)
    * @param rand The random generator used
    * @throws MathException MathException
    */
   public AbstractStableDistributionFunction(int dim, double w, RandomGenerator rand) throws MathException
   {
     super(dim, rand);
     reset(dim, w); 
   }

   public void reset(int dim, double w) throws MathException
   {
      RandomDataImpl dataSampler = new RandomDataImpl(rg);
      Sampler sampler = getSampler();      
      this.a = new double[dim];
      this.dim = dim;
      this.w = w;
      for(int i = 0;i < dim;++i)
      {
         a[i] = sampler.sample(dataSampler);
      }
      b = dataSampler.nextUniform(0, w);
   }


   /**
    * The sampler determines the metric which this LSH is associated with.
    * A 1-stable sample will yield a LSH which corresponds to a L1 metric; likewise for 2-stable and L2.
    * @return The sampler to use. 
    */
   protected abstract Sampler 
   getSampler();
   
   /**
    * Compute the LSH for a given vector.
    */
   public long apply(RealVector vector)
   {
     /*
      * The hash is just floor(<v, a>/w)
      */
      double ret = b;
     
      for(int i = 0;i < dim;++i)
      {
         ret += vector.getEntry(i)*a[i];
      }
      return (long)Math.floor(ret/w);
   } 
}
