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

package datafu.pig.hash.lsh.cosine;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.random.RandomGenerator;
import org.apache.commons.math.random.UnitSphereRandomVectorGenerator;

import datafu.pig.hash.lsh.interfaces.LSH;

/**
 * From wikipedia's article on <a href="http://en.wikipedia.org/wiki/Locality-sensitive_hashing" target="_blank">Locality Sensitive Hashing</a>:
 *
 * <pre>
 * Locality-sensitive hashing (LSH) is a method of performing probabilistic dimension reduction of high-dimensional data.
 * The basic idea is to hash the input items so that similar items are mapped to the same buckets with high probability
 * (the number of buckets being much smaller than the universe of possible input items).
 * </pre>
 *
 * <p>
 * In particular, this implementation implements a locality sensitive hashing scheme which maps high-dimensional vectors which are
 * close together (with high probability) according to <a href="http://en.wikipedia.org/wiki/Cosine_similarity" target="_blank">Cosine Similarity</a>
 * into the same buckets.  Each LSH maps a vector onto one side or the other of a random hyperplane, thereby producing a single
 * bit as the hash value.  Multiple, independent, hashes can be run on the same input and aggregated together to form a more
 * broad domain than a single bit.
 * </p>
 *
 * <p>
 * For more information, see Charikar, Moses S.. (2002). "Similarity Estimation Techniques from Rounding Algorithms". Proceedings of the 34th Annual ACM Symposium on Theory of Computing 2002.
 * </p>
 */
public class HyperplaneLSH extends LSH
{
    private RealVector r;

    /**
     * Locality sensitive hash that maps vectors onto 0,1 in such a way that colliding
     * vectors are "near" one another according to cosine similarity with high probability.
     *
     * <p>
     * Generally, multiple LSH are combined via repetition to increase the range of the hash function to the full set of longs.
     * This repetition is accomplished by wrapping instances of the LSH in a LSHFamily, which does the combination.
     * </p>
     *
     * <p>
     * The size of the hash family corresponds to the number of independent hashes you want to apply to the data.
     * In a k-near neighbors style of searching, this corresponds to the number of neighbors you want to find
     * (i.e. the number of vectors within a distance according to cosine similarity).
     * </p>
     *
     * @param dim The dimension of the vectors which are to be hashed
     * @param rg random number generator
     */
    public HyperplaneLSH(int dim, RandomGenerator rg)
    {
        super(dim, rg);

        UnitSphereRandomVectorGenerator generator = new UnitSphereRandomVectorGenerator(dim, rg);
        //compute our vector representing a hyperplane of dimension dim by taking a random vector
        //located on the unit sphere
        double[] normalVector = generator.nextVector();
        r = new ArrayRealVector(normalVector);
    }


    /**
     * Compute which side of the hyperplane that the parameter is on.  
     * 
     * @param vector The vector to test.
     * @return one if the dot product with the hyperplane is positive, 0 if negative.
     */
    public long apply(RealVector vector)
    {
        return r.dotProduct(vector) >= 0?1:0;
    }
    
}
