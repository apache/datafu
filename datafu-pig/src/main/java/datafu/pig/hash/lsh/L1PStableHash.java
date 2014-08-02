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

import org.apache.commons.math.MathException;
import org.apache.commons.math.random.RandomGenerator;

import datafu.pig.hash.lsh.interfaces.LSH;
import datafu.pig.hash.lsh.interfaces.LSHCreator;
import datafu.pig.hash.lsh.p_stable.L1LSH;

/**
 * From wikipedia's article on <a href="http://en.wikipedia.org/wiki/Locality-sensitive_hashing" target="_blank">Locality Sensitive Hashing</a>:
 * <pre>
 * Locality-sensitive hashing (LSH) is a method of performing probabilistic dimension reduction of high-dimensional data. 
 * The basic idea is to hash the input items so that similar items are mapped to the same buckets with high probability 
 * (the number of buckets being much smaller than the universe of possible input items).
 * </pre>
 * 
 * In particular, this implementation implements a locality sensitive hashing scheme which maps high-dimensional vectors which are
 * close together (with high probability) according to the <a href="http://en.wikipedia.org/wiki/Lp_space" target="_blank">L1</a>
 * distance metric into the same buckets.  This implementation uses a 1-stable distribution (a Cauchy distribution) in order
 * to accomplish this.
 * 
 * For more information, see Datar, M.; Immorlica, N.; Indyk, P.; Mirrokni, V.S. (2004). "Locality-Sensitive Hashing Scheme Based on p-Stable Distributions". Proceedings of the Symposium on Computational Geometry.
 * 
 */
public class L1PStableHash extends LSHFunc
{
    int dim;
    double w;
   
    int repeat;
    int numHashes;
    
    /**
     * Locality sensitive hash that maps vectors onto a long in such a way that colliding
     * vectors are "near" one another according to cosine similarity with high probability.  
     * 
     * <p>
     * Generally, multiple LSH are combined via repetition to increase the range of the hash function to the full set of longs.
     * The number of functions which you want to internally repeat is specified by the sRepeat parameter.
     * 
     * The size of the hash family corresponds to the number of independent hashes you want to apply to the data.
     * In a k-near neighbors style of searching, this corresponds to the number of neighbors you want to find
     * (i.e. the number of vectors within a distance according to cosine similarity).
     * 
     * This UDF, indeed all p-stable LSH functions, are parameterized with a quantization parameter (w or r in the literature
     * , depending on where you look).  Consider the following excerpt from Datar, M.; Immorlica, N.; Indyk, P.; Mirrokni, V.S. (2004). "Locality-Sensitive Hashing Scheme Based on p-Stable Distributions". Proceedings of the Symposium on Computational Geometry.
     * 
     * <pre>
     * Decreasing the width of the projection (w) decreases the probability of collision for any two points. 
     * Thus, it has the same effect as increasing k . As a result, we would like to set w as small as possible
     * and in this way decrease the number of projections we need to make. 
     * </pre>
     * 
     * In the literature, the quantization parameter (or width of the projection) is found empirically given a sample of
     * the data and the likely threshold of for the metric.  Tuning this parameter is very important for the performance
     * of this algorithm.
     * 
     * <p>
     * Consider the following example where we input some 3-dimensional points and a set of 3-dimensional queries
     * and find the nearest neighbors of the query points:
     * <pre>
     * -- Create a L1PStableHash of 
     * --   3 dimensional data
     * --   projection width of 150
     * --   1 internal hashes 
     * --   family of 5 hashes
     * --   with a seed of 0
     * 
     * -- This creates a bag of tuples:
     * --   lsh_id:Integer the family ID (in this case, 0-4)
     * --   hash:Long the hash 
     * 
     * define LSH datafu.pig.hash.lsh.L1PStableHash('3', '150', '1', '5', '0');
     * define METRIC datafu.pig.hash.lsh.metric.L1();
     *
     * PTS = LOAD 'input' AS (dim1:double, dim2:double, dim3:double);
     * 
     * --hash the input points
     * PTS_HASHED = foreach PTS generate TOTUPLE(dim1, dim2, dim3) as pt
     *                    , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)));
     * 
     * -- the hash family ID and the hash should group the input points into partitions
     * PARTITIONS = group PTS_HASHED by (lsh_id, hash);
     * 
     * -- take in the query points and hash them
     * QUERIES = LOAD 'queries' as (dim1:double, dim2:double, dim3:double);
     * QUERIES_HASHED = foreach QUERIES generate TOTUPLE(dim1, dim2, dim3) as query_pt
     *                        , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)))
     *                        ;
     * 
     * -- join the hashed query points with the (presumably larger) list of input data split by partitions
     * QUERIES_W_PARTS = join QUERIES_HASHED by (lsh_id, hash), PARTITIONS by (group.$0, group.$1);
     * 
     * -- Now, use the appropriate METRIC UDF (in this case L1 (aka city block) distance) to find the first point within
     * -- a parameterized threshold (in this case, 1000).  It takes:
     * --   query_pt:Tuple the query point
     * --   threshold:Double the threshold, so that if the distance between the query point and a point
     * --                    in the partition is less than this threshold, it returns the point (and stops searching)
     * --   partition:Bag The bag of tuples in the partition.
     * 
     *  
     * NEAR_NEIGHBORS = foreach QUERIES_W_PARTS generate query_pt as query_pt
     *                                                 , METRIC(query_pt, 1000, PTS_HASHED) as neighbor
     *                                                 ;
     * describe NEAR_NEIGHBORS;
     * -- {query_pt: (dim1: double,dim2: double,dim3: double)
     * -- ,neighbor: (pt: (dim1: double,dim2: double,dim3: double)
     * --            ,lsh::lsh_id: int
     * --            ,lsh::hash: long
     * --            )
     * -- }
     * 
     * -- project out the query and the matching point
     * NEIGHBORS_PROJ = foreach NEAR_NEIGHBORS {
     *  generate query_pt as query_pt, neighbor.pt as matching_pts;
     * };
     * 
     * -- Filter out the hashes which resulted in no matches
     * NOT_NULL = filter NEIGHBORS_PROJ by SIZE(matching_pts) &gt; 0;
     * 
     * -- group by the query
     * NEIGHBORS_GRP = group NOT_NULL by query_pt;
     * describe NEIGHBORS_GRP;
     * 
     * -- Generate the query, the number of matches and the bag of matching points
     * NEIGHBOR_CNT = foreach NEIGHBORS_GRP{
     *    DIST_MATCHING_PTS = DISTINCT MATCHING_PTS;
     *    generate group as query_pt, COUNT(NOT_NULL), DIST_MATCHING_PTS;
     * };
     * describe NEIGHBOR_CNT;
     * -- NEIGHBOR_CNT: {query_pt: (dim1: double,dim2: double,dim3: double)
     * --               ,long
     * --               ,DIST_MATCHING_PTS: { (matching_pts::dim1: double,matching_pts::dim2: double,matching_pts::dim3: double)
     * --                              }
     * --               }
     * STORE NEIGHBOR_CNT INTO 'neighbors';
     * </pre>
     * 
     * 
     * 
     * @param sDim  Dimension of the vectors
     * @param sW A double representing the quantization parameter (also known as the projection width)
     * @param sRepeat Number of internal repetitions (generally this should be 1 as the p-stable hashes have a larger range than one bit)
     * @param sNumHashes Size of the hash family (if you're looking for k near neighbors, this is the k)
     * @param sSeed Seed to use when constructing LSH family
     */
    public L1PStableHash(String sDim, String sW, String sRepeat, String sNumHashes, String sSeed)
    {
      super(sSeed);
      dim = Integer.parseInt(sDim);
      w = Double.parseDouble(sW);
      repeat = Integer.parseInt(sRepeat); 
      numHashes = Integer.parseInt(sNumHashes);
    }
    
    public L1PStableHash(String sDim, String sW, String sRepeat, String sNumHashes)
    {
       this(sDim, sW, sRepeat, sNumHashes, null);
    }

    @Override
    protected LSHCreator createLSHCreator() {
      return new LSHCreator(dim, numHashes, repeat, getSeed())
      {
  
        @Override
        protected LSH constructLSH(RandomGenerator rg) throws MathException {
          return new L1LSH(dim, w, rg );
        }
        
      };
    
    }
    @Override
    protected int getDimension() {
      return dim;
    }
}
