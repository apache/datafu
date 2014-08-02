/**
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
package datafu.test.pig.hash.lsh;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.math.MathException;
import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.random.JDKRandomGenerator;
import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.commons.math.random.RandomGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import datafu.pig.hash.lsh.LSHFamily;
import datafu.pig.hash.lsh.cosine.HyperplaneLSH;
import datafu.pig.hash.lsh.interfaces.LSH;
import datafu.pig.hash.lsh.interfaces.LSHCreator;
import datafu.pig.hash.lsh.metric.Cosine;
import datafu.pig.hash.lsh.metric.L1;
import datafu.pig.hash.lsh.metric.L2;
import datafu.pig.hash.lsh.p_stable.L1LSH;
import datafu.pig.hash.lsh.p_stable.L2LSH;

public class LSHTest {

  static RealVector getRandomVector(RandomData rd, double stddev, int dim)
  {
    RealVector vec = new ArrayRealVector(dim);
    for(int i =0;i < dim;++i)
    {
      vec.setEntry(i, rd.nextGaussian(0, stddev));
      
    }
    return vec;
  }
  
  static List<RealVector> getVectors(RandomData rd, double stddev, int n)
  {
    ArrayList<RealVector> vecs = new ArrayList<RealVector>();
    
    for(int i = 0;i < n;++i)
    {
      vecs.add(getRandomVector(rd, stddev, 3));
    }
    return vecs;
  }
  
  public List<Map<Long, List<RealVector> >> partition(List<RealVector> vectors, LSHFamily family, int k)
  {
    List<Map<Long, List<RealVector>>> partitions = new ArrayList<Map<Long, List<RealVector>>>();
    for(int i = 0; i < k;++i)
    {
      partitions.add(new HashMap<Long, List<RealVector>>());
    }
    for(RealVector vec : vectors)
    {
      int partitionId = 0;
      for(Long hash : family.apply(vec))
      {
        Map<Long, List<RealVector>> partition = partitions.get(partitionId);
        List<RealVector> partitionVectors = partition.get(hash);
        if(partitionVectors == null)
        {
          partitionVectors = new ArrayList<RealVector>();
          partition.put(hash, partitionVectors);
        }
        partitionVectors.add(vec);
        partitionId++;
      }
    }
    return partitions;
  }
  
  private static class QueryResults
  {
    int numResults;
    int numOps_naive;
    int numOps_lsh;
  }
  
  private static interface DistanceMetric
  {
    public double distance(RealVector v1, RealVector v2);
  }
  
  public QueryResults getNearest( RealVector query
                  , List<RealVector> rawVectors
                  , List<Map<Long, List<RealVector> >> partitions
                  , LSHFamily family
                  , DistanceMetric metric
                  , double threshold
                  )
  {
    QueryResults ret = new QueryResults();
    int partitionId = 0;
    for(Long hash : family.apply(query))
    {
      Map<Long, List<RealVector>> partition = partitions.get(partitionId);
      List<RealVector> vectors = partition.get(hash);
      if(vectors != null)
      {
        for(RealVector vec : vectors)
        {
          double dist = metric.distance(query, vec);
          ret.numOps_lsh++;
          if(dist < threshold)
          {
            ret.numResults++;
            break;
          }
        }
      }
      partitionId++;
    }
    
    boolean found = false;
    int total = 0;
    int numFound = 0;
    for(RealVector vec : rawVectors)
    {
      double dist = metric.distance(query, vec);
      if(!found)
      {
        ret.numOps_naive++;
      }
      if(dist < threshold)
      {
        if(!found)
        {
          numFound++;
          if(numFound == partitionId)
            found = true;
        }
        ++total;
      }
    }
    System.out.println(total + " vectors found in the set of " + rawVectors.size() + " (" + total*1.0/rawVectors.size() + ")");
    return ret;
  }
  private void harness( int k
            , List<Map<Long, List<RealVector> >> partitions
            , RandomData rd
            , List<RealVector> vectors
            , LSHFamily family
            , DistanceMetric metric
            , double threshold
            )
  {
    int partId = 0;
    for(Map<Long, List<RealVector> > partition : partitions)
    {
      System.out.println(partId + " => " + partition.size());
      ++partId;
    }
    int numResults = 0;
    int opsLessWithLSH = 0;
    for(int query = 0; query < 10;++query)
    {
      final RealVector queryVec = getRandomVector(rd, 1000, 3);
      
      QueryResults results = getNearest(queryVec
                       , vectors
                         , partitions
                         , family
                         , metric
                         , threshold
                         );
      
      
      numResults += results.numResults;
      opsLessWithLSH += results.numOps_naive - results.numOps_lsh;
      System.out.println("NumOperations = " + results.numOps_lsh + " vs " + results.numOps_naive);
      System.out.println("Found " + results.numResults + " of " + k);
      //Assert.assertTrue(results.numOps_lsh < results.numOps_naive);
      //Assert.assertEquals(results.numResults, k);
    }
    System.out.println("Avg Num Results = " + numResults/10.0);
    System.out.println("Avg ops less with LSH = " + opsLessWithLSH / 10.0);
    //ensure that we return at *least* half of the expected values
    Assert.assertTrue( numResults/10.0 > .5*k);
    //and that we're comparing fewer points with LSH versus brute force.
    Assert.assertTrue( opsLessWithLSH / 10.0 > 0);
  }
  @Test
  public void testCosineLSH() throws Exception
  {
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    List<RealVector> vectors = getVectors(rd, 1000, 1000);
    int k = 5;
    LSHCreator creator = new LSHCreator(3, k, 1500, 0) {
      
      @Override
      protected LSH constructLSH(RandomGenerator rg) throws MathException {
        return new HyperplaneLSH(getDim(), rg);
      }
    };
    LSHFamily family = creator.constructFamily(rg);
    List<Map<Long, List<RealVector> >> partitions = partition(vectors, family, creator.getNumHashes());
    DistanceMetric metric = new DistanceMetric()
    {
      public double distance(RealVector v1, RealVector v2)
      {
        return Cosine.distance(v1, v2);
      }
    };
    harness(k, partitions, rd, vectors, family, metric, .001);
  }
  
  @Test
  public void testL1LSH() throws Exception
  {
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    List<RealVector> vectors = getVectors(rd, 1000, 1000);
    int k = 5;
    LSHCreator creator = new LSHCreator(3, k, 1, 0) {
      
      @Override
      protected LSH constructLSH(RandomGenerator rg) throws MathException {
        return new L1LSH(getDim(), 150, rg);
      }
    };
    LSHFamily family = creator.constructFamily(rg);
    List<Map<Long, List<RealVector> >> partitions = partition(vectors, family, creator.getNumHashes());
    DistanceMetric metric = new DistanceMetric()
    {
      public double distance(RealVector v1, RealVector v2)
      {
        return L1.distance(v1, v2);
      }
    };
    harness(k, partitions, rd, vectors, family, metric, 1000);
  }
  
  @Test
  public void testL2LSH() throws Exception
  {
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    List<RealVector> vectors = getVectors(rd, 1000, 1000);
    int k = 5;
    LSHCreator creator = new LSHCreator(3, k, 1, 0) {
      
      @Override
      protected LSH constructLSH(RandomGenerator rg) throws MathException {
        return new L2LSH(getDim(), 200, rg);
      }
    };
    LSHFamily family = creator.constructFamily(rg);
    List<Map<Long, List<RealVector> >> partitions = partition(vectors, family, creator.getNumHashes());
    DistanceMetric metric = new DistanceMetric()
    {
      public double distance(RealVector v1, RealVector v2)
      {
        return L2.distance(v1, v2);
      }
    };
    harness(k, partitions, rd, vectors, family, metric, 1000);
  }
}
