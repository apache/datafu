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

package datafu.test.pig.hash;

/**
 * class with a fixed random seed for testing purposes
 */
public class HasherRandForTesting extends datafu.pig.hash.HasherRand
{
  public HasherRandForTesting(String alg) {
    super(alg);
  }
  /* always yield the same seed. */
  @Override
  protected java.util.Random getRandomGenerator() {
    return new java.util.Random(69);
  }
}
