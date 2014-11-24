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

import org.apache.commons.math.linear.RealVector;

/**
 * A UDF used to find a vector v in a bag such that for query point q, metric m and threshold t
 * m(v,q) &lt; t.  In other words, find the first vector in the bag within a threshold distance away.
 * 
 *  It returns one of the tuples of the bag of vectors.  The metric used is 
 * <a href="http://en.wikipedia.org/wiki/Cosine_similarity" target="_blank">Cosine Similarity</a>, 
 * which technically does not form a metric, but I'm stretching the definition here.
 * 
 * @see datafu.pig.hash.lsh.CosineDistanceHash CosineDistanceHash for an example
 */
public class Cosine extends MetricUDF {
  
  /**
   * Create a new Cosine Metric UDF with a given dimension.
   * 
   * @param sDim dimension
   */
  public Cosine(String sDim) {
    super(sDim); 
  }
  
  /**
   * Cosine similarity.
   * @param v1 first vector
   * @param v2 second vector
   * @return The cosine of the angle between the vectors
   */
  public static double distance(RealVector v1, RealVector v2) {
    return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
  }

  /**
   * Cosine similarity.
   * @param v1 first vector
   * @param v2 second vector
   * @return Roughly the cosine of the angle between the vectors
   */
  @Override
  protected double dist(RealVector v1, RealVector v2) {
    return distance(v1, v2);
  }

}
