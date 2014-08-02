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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.commons.math.linear.RealVector;
import org.apache.commons.math.random.JDKRandomGenerator;
import org.apache.commons.math.random.RandomData;
import org.apache.commons.math.random.RandomDataImpl;
import org.apache.commons.math.random.RandomGenerator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.apache.pig.tools.parameters.ParseException;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

import datafu.pig.hash.lsh.metric.Cosine;
import datafu.pig.hash.lsh.metric.L1;
import datafu.pig.hash.lsh.metric.L2;
import datafu.pig.hash.lsh.util.DataTypeUtil;
import datafu.test.pig.PigTests;

public class LSHPigTest extends PigTests
{
  
  private static void setDebuggingLogging()
  {
    Logger.getRootLogger().setLevel(Level.INFO);
    Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
  }
  
  private static void setMemorySettings()
  {
    System.getProperties().setProperty("mapred.map.child.java.opts", "-Xmx1G");
    System.getProperties().setProperty("mapred.reduce.child.java.opts","-Xmx1G");
    System.getProperties().setProperty("io.sort.mb","10");
  }
  
  /**
   * PTS = LOAD 'input' AS (b:bag{t:tuple(idx:int, val:double)});
   * STORE PTS INTO 'output';
   */
  @Multiline private String sparseVectorTest;
  
  @Test
  public void testSparseVectors() throws IOException, ParseException
  {
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    int n = 20;
    List<RealVector> vectors = LSHTest.getVectors(rd, 1000, n);
    PigTest test = createPigTestFromString(sparseVectorTest);
    writeLinesToFile("input", getSparseLines(vectors));
    test.runScript();
    List<Tuple> neighbors = this.getLinesForAlias(test, "PTS");
    Assert.assertEquals(neighbors.size(), n);
    int idx = 0;
    for(Tuple t : neighbors)
    {
      Assert.assertTrue(t.get(0) instanceof DataBag);
      Assert.assertEquals(t.size(), 1);
      RealVector interpreted = DataTypeUtil.INSTANCE.convert(t, 3);
      RealVector original = vectors.get(idx);
      Assert.assertEquals(original.getDimension(), interpreted.getDimension());
      for(int i = 0;i < interpreted.getDimension();++i)
      {
        double originalField = original.getEntry(i);
        double interpretedField = interpreted.getEntry(i);
        Assert.assertTrue(Math.abs(originalField - interpretedField) < 1e-5);
      }
      
      idx++;
    }
  }
  
/**
  
  define LSH datafu.pig.hash.lsh.L1PStableHash('3', '150', '1', '5');
  define METRIC datafu.pig.hash.lsh.metric.L1('3');
  
  PTS = LOAD 'input' AS (dim1:double, dim2:double, dim3:double);
  PTS_HASHED = foreach PTS generate TOTUPLE(dim1, dim2, dim3) as pt
                                  , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)));
  
  store PTS_HASHED INTO 'lsh_pts'; 
  */
  @Multiline private String randomSeedTest;
  @Test
  public void testRandomSeed() throws Exception
  {
    setMemorySettings();
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    int n = 10;
    List<RealVector> vectors = LSHTest.getVectors(rd, 1000, n);
    writeLinesToFile("input", getLines(vectors));
    Map<String, Long> hashes = new HashMap<String, Long>();
    int numDiff = 0;
    int numHashes = 0;
    {
      PigTest test = createPigTestFromString(randomSeedTest);
      test.runScript();
      List<Tuple> pts = this.getLinesForAlias(test, "PTS_HASHED");
      numHashes = pts.size();
      for(Tuple t : pts)
      {
        String key = ((Tuple)t.get(0)).toString() + t.get(1).toString();
        Long value = (Long)t.get(2);
        hashes.put(key, value);
      }
    }
    {
      PigTest test = createPigTestFromString(randomSeedTest);
      test.runScript();
      List<Tuple> pts = this.getLinesForAlias(test, "PTS_HASHED");
      Assert.assertEquals(numHashes, pts.size());
      for(Tuple t : pts)
      {
        String key = ((Tuple)t.get(0)).toString() + t.get(1).toString();
        Long value = (Long)t.get(2);
        Long refValue = hashes.get(key);
        if(!value.equals(refValue))
        {
          numDiff++;
        }
      }
    }
    //Assert that 80% of the hashes are different between runs due to different seeds.
    //this ensures that a random seed is actually being used.
    System.out.println(1.0*numDiff / numHashes);
    Assert.assertTrue(1.0*numDiff/numHashes > .8);
  }
  
   /**
  
  define LSH datafu.pig.hash.lsh.L1PStableHash('3', '150', '1', '5', '0');
  define METRIC datafu.pig.hash.lsh.metric.L1('3');
  
  PTS = LOAD 'input' AS (pt:bag{t:tuple(idx:int, val:double)});
  PTS_HASHED = foreach PTS generate pt as pt
                                  , FLATTEN(LSH(pt));
  PARTITIONS = group PTS_HASHED by (lsh_id, hash);
  
  QUERIES = LOAD 'queries' as (pt:bag{t:tuple(idx:int, val:double)});
  QUERIES_HASHED = foreach QUERIES generate pt as query_pt
                      , FLATTEN(LSH(pt))
                      ;
  QUERIES_W_PARTS = join QUERIES_HASHED by (lsh_id, hash), PARTITIONS by (group.$0, group.$1);
  NEAR_NEIGHBORS = foreach QUERIES_W_PARTS generate query_pt as query_pt
                              , METRIC(query_pt, 1000, PTS_HASHED) as neighbor
                              ;
 
 describe NEAR_NEIGHBORS;
  NEIGHBORS_PROJ = foreach NEAR_NEIGHBORS {
   
   generate TOTUPLE(query_pt) as query_pt, neighbor.pt as matching_pts;
  };
  describe NEIGHBORS_PROJ;
  NOT_NULL = filter NEIGHBORS_PROJ by SIZE(matching_pts) > 0;
  NEIGHBORS_GRP = group NOT_NULL by query_pt;
  describe NEIGHBORS_GRP;
  
  NEIGHBOR_CNT = foreach NEIGHBORS_GRP{
   MATCHING_PTS = foreach NOT_NULL generate matching_pts;
   DIST_MATCHING_PTS = DISTINCT MATCHING_PTS;
   generate group as query_pt, COUNT(NOT_NULL), DIST_MATCHING_PTS;
  };
  STORE NEIGHBOR_CNT INTO 'neighbors';
  
  
  */
 @Multiline private String l1SparseTest;
 
 @Test
 public void testL1UDFSparse() throws Exception
 {
   
   setMemorySettings();
   RandomGenerator rg = new JDKRandomGenerator();
   rg.setSeed(0);
   RandomData rd = new RandomDataImpl(rg);
   int n = 1000;
   List<RealVector> vectors = LSHTest.getVectors(rd, 1000, n);
   PigTest test = createPigTestFromString(l1SparseTest);
   writeLinesToFile("input", getSparseLines(vectors));
   List<RealVector> queries = LSHTest.getVectors(rd, 1000, 10);
   writeLinesToFile("queries", getSparseLines(queries));
   test.runScript();
   List<Tuple> neighbors = this.getLinesForAlias(test, "NEIGHBOR_CNT");
   Assert.assertEquals( queries.size(), neighbors.size() );
   for(long cnt : getCounts(neighbors))
   {
     Assert.assertTrue(cnt >= 3);
   }
   Distance d = new Distance()
   {

     @Override
     public double distance(RealVector v1, RealVector v2) {
       return L1.distance(v1, v2);
     }
     
   };
   verifyPoints(neighbors, d, 1000);
 }
  
  /**
   
   define LSH datafu.pig.hash.lsh.L1PStableHash('3', '150', '1', '5', '0');
   define METRIC datafu.pig.hash.lsh.metric.L1('3');
   
   PTS = LOAD 'input' AS (dim1:double, dim2:double, dim3:double);
   PTS_HASHED = foreach PTS generate TOTUPLE(dim1, dim2, dim3) as pt
                                   , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)));
   PARTITIONS = group PTS_HASHED by (lsh_id, hash);
   
   QUERIES = LOAD 'queries' as (dim1:double, dim2:double, dim3:double);
   QUERIES_HASHED = foreach QUERIES generate TOTUPLE(dim1, dim2, dim3) as query_pt
                       , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)))
                       ;
   QUERIES_W_PARTS = join QUERIES_HASHED by (lsh_id, hash), PARTITIONS by (group.$0, group.$1);
   NEAR_NEIGHBORS = foreach QUERIES_W_PARTS generate query_pt as query_pt
                               , METRIC(query_pt, 1000, PTS_HASHED) as neighbor
                               ;
  
  describe NEAR_NEIGHBORS;
   NEIGHBORS_PROJ = foreach NEAR_NEIGHBORS {
    
    generate query_pt as query_pt, neighbor.pt as matching_pts;
   };
   describe NEIGHBORS_PROJ;
   NOT_NULL = filter NEIGHBORS_PROJ by SIZE(matching_pts) > 0;
   NEIGHBORS_GRP = group NOT_NULL by query_pt;
   describe NEIGHBORS_GRP;
   
   NEIGHBOR_CNT = foreach NEIGHBORS_GRP{
    MATCHING_PTS = foreach NOT_NULL generate FLATTEN(matching_pts);
    DIST_MATCHING_PTS = DISTINCT MATCHING_PTS;
    generate group as query_pt, COUNT(NOT_NULL), DIST_MATCHING_PTS;
   };
   STORE NEIGHBOR_CNT INTO 'neighbors';
   
   
   */
  @Multiline private String l1Test;
  
  @Test
  public void testL1UDF() throws Exception
  {
    setMemorySettings();
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    int n = 1000;
    List<RealVector> vectors = LSHTest.getVectors(rd, 1000, n);
    PigTest test = createPigTestFromString(l1Test);
    writeLinesToFile("input", getLines(vectors));
    List<RealVector> queries = LSHTest.getVectors(rd, 1000, 10);
    writeLinesToFile("queries", getLines(queries));
    test.runScript();
    List<Tuple> neighbors = this.getLinesForAlias(test, "NEIGHBOR_CNT");
    Assert.assertEquals( queries.size(), neighbors.size() );
    for(long cnt : getCounts(neighbors))
    {
      Assert.assertTrue(cnt >= 3);
    }
    Distance d = new Distance()
    {

      @Override
      public double distance(RealVector v1, RealVector v2) {
        return L1.distance(v1, v2);
      }
      
    };
    verifyPoints(neighbors, d, 1000);
  }
  
  /**
   
   define LSH datafu.pig.hash.lsh.L2PStableHash('3', '200', '1', '5', '0');
   define METRIC datafu.pig.hash.lsh.metric.L2('3');
   
   PTS = LOAD 'input' AS (dim1:double, dim2:double, dim3:double);
   PTS_HASHED = foreach PTS generate TOTUPLE(dim1, dim2, dim3) as pt
                   , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)));
   PARTITIONS = group PTS_HASHED by (lsh_id, hash);
   
   QUERIES = LOAD 'queries' as (dim1:double, dim2:double, dim3:double);
   QUERIES_HASHED = foreach QUERIES generate TOTUPLE(dim1, dim2, dim3) as query_pt
                       , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)))
                       ;
   QUERIES_W_PARTS = join QUERIES_HASHED by (lsh_id, hash), PARTITIONS by (group.$0, group.$1);
   NEAR_NEIGHBORS = foreach QUERIES_W_PARTS generate query_pt as query_pt
                               , METRIC(query_pt, 1000, PTS_HASHED) as neighbor
                               ;
   describe NEAR_NEIGHBORS;
   NEIGHBORS_PROJ = foreach NEAR_NEIGHBORS {
    
    generate query_pt as query_pt, neighbor.pt as matching_pts;
   };
   describe NEIGHBORS_PROJ;
   NOT_NULL = filter NEIGHBORS_PROJ by SIZE(matching_pts) > 0;
   NEIGHBORS_GRP = group NOT_NULL by query_pt;
   describe NEIGHBORS_GRP;
   
   NEIGHBOR_CNT = foreach NEIGHBORS_GRP{
    MATCHING_PTS = foreach NOT_NULL generate FLATTEN(matching_pts);
    DIST_MATCHING_PTS = DISTINCT MATCHING_PTS;
    generate group as query_pt, COUNT(NOT_NULL), DIST_MATCHING_PTS;
   };
   STORE NEIGHBOR_CNT INTO 'neighbors';
   
   
   */
  @Multiline private String l2Test;
  
  @Test
  public void testL2UDF() throws Exception
  {
    setMemorySettings();
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    int n = 1000;
    List<RealVector> vectors = LSHTest.getVectors(rd, 1000, n);
    PigTest test = createPigTestFromString(l2Test);
    writeLinesToFile("input", getLines(vectors));
    List<RealVector> queries = LSHTest.getVectors(rd, 1000, 10);
    writeLinesToFile("queries", getLines(queries));
    test.runScript();
    List<Tuple> neighbors = this.getLinesForAlias(test, "NEIGHBOR_CNT");
    Assert.assertEquals( queries.size(), neighbors.size() );
    for(long cnt : getCounts(neighbors))
    {
      Assert.assertTrue(cnt >= 3);
    }
    Distance d = new Distance()
    {

      @Override
      public double distance(RealVector v1, RealVector v2) {
        return L2.distance(v1, v2);
      }
      
    };
    verifyPoints(neighbors, d, 1000);
  }
  
  /**
   
   define LSH datafu.pig.hash.lsh.CosineDistanceHash('3', '1500', '5', '0');
   define METRIC datafu.pig.hash.lsh.metric.Cosine('3');
   
   PTS = LOAD 'input' AS (dim1:double, dim2:double, dim3:double);
   PTS_HASHED = foreach PTS generate TOTUPLE(dim1, dim2, dim3) as pt
                   , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)));
   PARTITIONS = group PTS_HASHED by (lsh_id, hash);
   
   QUERIES = LOAD 'queries' as (dim1:double, dim2:double, dim3:double);
   QUERIES_HASHED = foreach QUERIES generate TOTUPLE(dim1, dim2, dim3) as query_pt
                       , FLATTEN(LSH(TOTUPLE(dim1, dim2, dim3)))
                       ;
   describe QUERIES_HASHED;
   QUERIES_W_PARTS = join QUERIES_HASHED by (lsh_id, hash), PARTITIONS by (group.$0, group.$1);
   NEAR_NEIGHBORS = foreach QUERIES_W_PARTS generate query_pt as query_pt
                               , METRIC(query_pt, .001, PTS_HASHED) as neighbor
                               ;
   describe NEAR_NEIGHBORS;
   NEIGHBORS_PROJ = foreach NEAR_NEIGHBORS {
    
    generate query_pt as query_pt, neighbor.pt as matching_pts;
   };
   describe NEIGHBORS_PROJ;
   NOT_NULL = filter NEIGHBORS_PROJ by SIZE(matching_pts) > 0;
   NEIGHBORS_GRP = group NOT_NULL by query_pt;
   describe NEIGHBORS_GRP;
   
   NEIGHBOR_CNT = foreach NEIGHBORS_GRP{
    MATCHING_PTS = foreach NOT_NULL generate FLATTEN(matching_pts);
    DIST_MATCHING_PTS = DISTINCT MATCHING_PTS;
      generate group as query_pt, COUNT(NOT_NULL), DIST_MATCHING_PTS;
   };
   describe NEIGHBOR_CNT;
   STORE NEIGHBOR_CNT INTO 'neighbors';
   
   
   */
  @Multiline private String cosTest;
  
  @Test
  public void testCosineUDF() throws Exception
  {
    setMemorySettings();
    RandomGenerator rg = new JDKRandomGenerator();
    rg.setSeed(0);
    RandomData rd = new RandomDataImpl(rg);
    int n = 1000;
    List<RealVector> vectors = LSHTest.getVectors(rd, 1000, n);
    PigTest test = createPigTestFromString(cosTest);
    writeLinesToFile("input", getLines(vectors));
    List<RealVector> queries = LSHTest.getVectors(rd, 1000, 10);
    writeLinesToFile("queries", getLines(queries));
    test.runScript();
    List<Tuple> neighbors = this.getLinesForAlias(test, "NEIGHBOR_CNT");
    Assert.assertEquals( queries.size(), neighbors.size() );
    for(long cnt : getCounts(neighbors))
    {
      Assert.assertTrue(cnt >= 2);
    }
    Distance d = new Distance()
    {

      @Override
      public double distance(RealVector v1, RealVector v2) {
        return Cosine.distance(v1, v2);
      }
      
    };
    verifyPoints(neighbors, d, .001);
  }
  private static interface Distance
  {
    public double distance(RealVector v1, RealVector v2);
  }
  
  private void verifyPoints(List<Tuple> neighbors, Distance d, double threshold) throws PigException
  {
    for(Tuple t : neighbors)
    {
      RealVector queryPt = DataTypeUtil.INSTANCE.convert(t, 3);
      DataBag bag = (DataBag) t.get(2);
      for(Tuple neighbor : bag)
      {
        RealVector v = DataTypeUtil.INSTANCE.convert(neighbor, 3);
        double distance = d.distance(queryPt, v);
        Assert.assertTrue(distance < threshold);
      }
    }
  }
  
  private Iterable<Long> getCounts(List<Tuple> neighbors)
  {
    return Iterables.transform(neighbors, new Function<Tuple, Long>()
        {
      public Long apply(Tuple in)
      {
        try {
          return (Long)in.get(1);
        } catch (ExecException e) {
          return -1L;
        }
      }
        });
  }
  private String[] getSparseLines(List<RealVector> vectors)
  {
    String[] input = new String[vectors.size()];
    int i = 0;
    for(RealVector vec : vectors)
    {
      input[i++] = String.format("({(%d,%f),(%d,%f),(%d,%f)})", 0, vec.getEntry(0), 1, vec.getEntry(1), 2, vec.getEntry(2));
    }
    return input;
  }
  
  private String[] getLines(List<RealVector> vectors)
  {
    String[] input = new String[vectors.size()];
    int i = 0;
    for(RealVector vec : vectors)
    {
      input[i++] = String.format("%f\t%f\t%f", vec.getEntry(0), vec.getEntry(1), vec.getEntry(2));
    }
    return input;
  }
}
