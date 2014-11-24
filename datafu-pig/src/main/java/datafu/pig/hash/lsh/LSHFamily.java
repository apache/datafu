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

import org.apache.commons.math.linear.RealVector;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import datafu.pig.hash.lsh.interfaces.LSH;

/**
 * A family of k locality sensitive hashes.  For a given point, k hashes will be computed.
 */
public class LSHFamily {

  private List<LSH> hashes;
  /**
   * Construct a family of hashes
   * @param hashes Hashes which will be applied in turn to a given point
   */
  public LSHFamily(List<LSH> hashes) 
  {
    this.hashes = hashes;
  }

  /**
   * Compute the family of k-hashes for a vector.
   * 
   * @param vector the vector
   * @return An iterable of hashes
   */
  public Iterable<Long> apply(final RealVector vector) {
    return Iterables.transform(hashes, new Function<LSH, Long>()
        {
          @Override
          public Long apply(LSH lsh)
          {
            return lsh.apply(vector);
          }
        }
                 );
  }

}
