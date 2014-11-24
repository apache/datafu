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
 * <p>
 * It returns one of the tuples of the bag of vectors using <a href="http://en.wikipedia.org/wiki/Lp_space" target="_blank">L2 distance</a>, 
 * distance between two vectors.  This is otherwise known as
 * the Euclidean distance.
 * </p>
 *
 * @see datafu.pig.hash.lsh.L2PStableHash L2PStableHash for an example
 */
public class L2 extends MetricUDF {

  /**
   * Create a new L2 Metric UDF with a given dimension.
   * 
   * @param sDim dimension
   */
  public L2(String sDim) {
    super(sDim);
   
  }

  public static double distance(RealVector v1, RealVector v2) {
    return v1.getDistance(v2);
  }
  
  @Override
  protected double dist(RealVector v1, RealVector v2) {
    return distance(v1, v2);
  }
}
