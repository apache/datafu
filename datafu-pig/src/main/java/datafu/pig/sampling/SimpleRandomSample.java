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

package datafu.pig.sampling;

import java.io.IOException;
import java.util.Comparator;

import org.apache.commons.math.random.RandomDataImpl;
import org.apache.pig.AlgebraicEvalFunc;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Scalable simple random sampling (ScaSRS).
 *
 * <p>
 * This UDF implements a scalable simple random sampling algorithm described in
 * </p>
 *
 * <pre>
 * X. Meng, Scalable Simple Random Sampling and Stratified Sampling, ICML 2013.
 * </pre>
 *
 * <p>
 * It takes a bag of n items and a sampling probability p as the inputs, and outputs a
 * simple random sample of size exactly ceil(p*n) in a bag, with probability at least
 * 99.99%. For example, the following script generates a simple random sample with
 * sampling probability 0.1:
 * </p>
 *
 * <pre>
 * DEFINE SRS datafu.pig.sampling.SimpleRandomSample();
 *
 * item    = LOAD 'input' AS (x:double);
 * sampled = FOREACH (GROUP item ALL) GENERATE FLATTEN(SRS(item, 0.01));
 * </pre>
 *
 * <p>
 * Optionally, user can provide a good lower bound of n as the third argument to help
 * reduce the size of intermediate data, for example:
 * </p>
 *
 * <pre>
 * DEFINE SRS datafu.pig.sampling.SimpleRandomSample();
 *
 * item    = LOAD 'input' AS (x:double);
 * summary = FOREACH (GROUP item ALL) GENERATE COUNT(item) AS count;
 * sampled = FOREACH (GROUP item ALL) GENERATE FLATTEN(SRS(item, 0.01, summary.count));
 * </pre>
 *
 * <p>
 * This UDF is very useful for stratified sampling. For example, the following script
 * keeps all positive examples while downsampling negatives with probability 0.1:
 * </p>
 *
 * <pre>
 * DEFINE SRS datafu.pig.sampling.SimpleRandomSample();
 *
 * item    = LOAD 'input' AS (x:double, label:int);
 * grouped = FOREACH (GROUP item BY label) GENERATE item, (group == 1 ? 1.0 : 0.1) AS p;
 * sampled = FOREACH grouped GENERATE FLATTEN(SRS(item, p));
 * </pre>
 *
 * <p>
 * In a Java Hadoop MapReduce job, we can output selected items directly using
 * MultipleOutputs. However, this feature is not available in a Pig UDF. So we still let
 * selected items go through the sort phase. However, as long as the sample size is not
 * huge, this should not be a big problem.
 * </p>
 *
 * <p>
 * In the first version, the sampling probability is specified in the constructor. This
 * method is deprecated now and will be removed in the next release.
 * </p>
 *
 */
public class SimpleRandomSample extends AlgebraicEvalFunc<DataBag>
{
  /**
   * Prefix for the output bag name.
   */
  public static final String OUTPUT_BAG_NAME_PREFIX = "SRS";

  private static final TupleFactory _TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory _BAG_FACTORY = BagFactory.getInstance();

  public SimpleRandomSample()
  {
    // empty
  }

  /**
   * Constructs this UDF with a sampling probability.
   *
   * @param samplingProbability sampling probability
   * @deprecated Should specify the sampling probability in the function call.
   */
  @Deprecated
  public SimpleRandomSample(String samplingProbability)
  {
	double p = Double.parseDouble(samplingProbability);
	verifySamplingProbability(p);
  }
  
  @Override
  public String getInitial()
  {
    return Initial.class.getName();
  }

  @Override
  public String getIntermed()
  {
    return Intermediate.class.getName();
  }

  @Override
  public String getFinal()
  {
    return Final.class.getName();
  }

  @Override
  public Schema outputSchema(Schema input)
  {
    try
    {
      Schema.FieldSchema inputFieldSchema = input.getField(0);

      if (inputFieldSchema.type != DataType.BAG)
      {
        throw new RuntimeException("Expected a BAG as input");
      }

      return new Schema(new Schema.FieldSchema(super.getSchemaName(OUTPUT_BAG_NAME_PREFIX,
                                                                   input),
                                               inputFieldSchema.schema,
                                               DataType.BAG));
    }
    catch (FrontendException e)
    {
      throw new RuntimeException(e);
    }
  }

  static public class Initial extends EvalFunc<Tuple>
  {
    // Should avoid creating many random number generator instances.
    private static RandomDataImpl _RNG = new RandomDataImpl();

    synchronized private static double nextDouble()
    {
      return _RNG.nextUniform(0.0d, 1.0d);
    }

    public Initial()
    {
      // empty
    }

    @Deprecated
    public Initial(String samplingProbability)
    {
      _p = Double.parseDouble(samplingProbability);
    }
    
    private boolean _first = true;
    private double _p = -1.0d; // the sampling probability
    private long _n1 = 0L; // the input lower bound of the size of the population
    private long _localCount = 0L; // number of items processed by this instance

    @Override
    public Tuple exec(Tuple input) throws IOException
    {
      int numArgs = input.size();
      
      // The first if clause is for backward compatibility, which should be removed 
      // after we remove specifying sampling probability in the constructor.
      if(numArgs == 1)
      {
        if(_p < 0.0d)
        {
          throw new IllegalArgumentException("Sampling probability is not given.");
        }
      }
      else if (numArgs < 2 || numArgs > 3)
      {
        throw new IllegalArgumentException("The input tuple should have either two or three fields: "
            + "a bag of items, the sampling probability, "
            + "and optionally a good lower bound of the size of the population or the exact number.");
      }

      DataBag items = (DataBag) input.get(0);
      long numItems = items.size();
      _localCount += numItems;

      // This is also for backward compatibility. Should change to
      // double p = ((Number) input.get(1)).doubleValue();
      // after we remove specifying sampling probability in the constructor.
      double p = numArgs == 1 ? _p : ((Number) input.get(1)).doubleValue();
      if (_first)
      {
        _p = p;
        verifySamplingProbability(p);
      }
      else
      {
        if (p != _p)
        {
          throw new IllegalArgumentException("The sampling probability must be a scalar, but found two different values: "
              + _p + " and " + p + ".");
        }
      }

      long n1 = 0L;
      if (numArgs > 2)
      {
        n1 = ((Number) input.get(2)).longValue();

        if (_first)
        {
          _n1 = n1;
        }
        else
        {
          if (n1 != _n1)
          {
            throw new IllegalArgumentException("The lower bound of the population size must be a scalar, but found two different values: "
                + _n1 + " and " + n1 + ".");
          }
        }
      }

      _first = false;

      // Use the local count if the input lower bound is smaller.
      n1 = Math.max(n1, _localCount);

      DataBag selected = _BAG_FACTORY.newDefaultBag();
      DataBag waiting = _BAG_FACTORY.newDefaultBag();

      if (n1 > 0L)
      {
        double q1 = getQ1(n1, p);
        double q2 = getQ2(n1, p);

        for (Tuple t : items)
        {
          double x = nextDouble();
          if (x < q1)
          {
            selected.add(t);
          }
          else if (x < q2)
          {
            waiting.add(new ScoredTuple(x, t).getIntermediateTuple(_TUPLE_FACTORY));
          }
        }
      }

      /*
       * The output tuple contains the following fields: sampling probability (double),
       * number of processed items in this tuple (long), a good lower bound of the size of
       * the population or the exact number (long), a bag of selected items (bag), and a
       * bag of waitlisted items with scores (bag).
       */
      Tuple output = _TUPLE_FACTORY.newTuple();

      output.append(p);
      output.append(numItems);
      output.append(n1);
      output.append(selected);
      output.append(waiting);

      return output;
    }
  }

  public static class Intermediate extends EvalFunc<Tuple>
  {
    public Intermediate()
    {
      // empty
    }
    
    @Deprecated
    public Intermediate(String samplingProbability)
    {
      // empty
    }

    @Override
    public Tuple exec(Tuple input) throws IOException
    {
      DataBag bag = (DataBag) input.get(0);

      DataBag selected = _BAG_FACTORY.newDefaultBag();
      DataBag aggWaiting = _BAG_FACTORY.newDefaultBag();

      boolean first = true;
      double p = 0.0d;
      long numItems = 0L; // number of items processed, including rejected
      long n1 = 0L;

      for (Tuple tuple : bag)
      {
        if (first)
        {
          p = (Double) tuple.get(0);
          first = false;
        }

        numItems += (Long) tuple.get(1);
        n1 = Math.max((Long) tuple.get(2), numItems);

        selected.addAll((DataBag) tuple.get(3));
        aggWaiting.addAll((DataBag) tuple.get(4));
      }

      DataBag waiting = _BAG_FACTORY.newDefaultBag();

      if (n1 > 0L)
      {
        double q1 = getQ1(n1, p);
        double q2 = getQ2(n1, p);

        for (Tuple t : aggWaiting)
        {
          ScoredTuple scored = ScoredTuple.fromIntermediateTuple(t);

          if (scored.getScore() < q1)
          {
            selected.add(scored.getTuple());
          }
          else if (scored.getScore() < q2)
          {
            waiting.add(t);
          }
        }
      }

      Tuple output = _TUPLE_FACTORY.newTuple();

      output.append(p);
      output.append(numItems);
      output.append(n1);
      output.append(selected);
      output.append(waiting);

      return output;
    }
  }

  static public class Final extends EvalFunc<DataBag>
  {
    public Final()
    {
      // empty
    }

    @Deprecated
    public Final(String samplingProbability)
    {
      // empty
    }
    
    @Override
    public DataBag exec(Tuple input) throws IOException
    {
      DataBag bag = (DataBag) input.get(0);

      boolean first = true;
      double p = 0.0d; // the sampling probability
      long n = 0L; // the size of the population (total number of items)

      DataBag selected = _BAG_FACTORY.newDefaultBag();
      DataBag waiting = _BAG_FACTORY.newSortedBag(ScoredTupleComparator.getInstance());

      for (Tuple tuple : bag)
      {
        if (first)
        {
          p = (Double) tuple.get(0);
          first = false;
        }

        n += (Long) tuple.get(1);
        selected.addAll((DataBag) tuple.get(3));
        waiting.addAll((DataBag) tuple.get(4));
      }

      long numSelected = selected.size();
      long numWaiting = waiting.size();

      long s = (long) Math.ceil(p * n); // sample size

      System.out.println("To sample " + s + " items from " + n + ", we pre-selected "
          + numSelected + ", and waitlisted " + waiting.size() + ".");

      long numNeeded = s - selected.size();

      if (numNeeded < 0)
      {
        System.err.println("Pre-selected " + numSelected + " items, but only needed " + s
            + ".");
      }

      for (Tuple scored : waiting)
      {
        if (numNeeded <= 0)
        {
          break;
        }
        selected.add(ScoredTuple.fromIntermediateTuple(scored).getTuple());
        numNeeded--;
      }

      if (numNeeded > 0)
      {
        System.err.println("The waiting list only has " + numWaiting
            + " items, but needed " + numNeeded + " more.");
      }

      return selected;
    }
  }

  // computes a threshold to select items
  private static double getQ1(long n, double p)
  {
    double t1 = 20.0d / (3.0d * n);
    double q1 = p + t1 - Math.sqrt(t1 * t1 + 3.0d * t1 * p);
    return q1;
  }

  // computes a threshold to reject items
  private static double getQ2(long n, double p)
  {
    double t2 = 10.0d / n;
    double q2 = p + t2 + Math.sqrt(t2 * t2 + 2.0d * t2 * p);
    return q2;
  }
  
  private static void verifySamplingProbability(double p)
  {
	if(p < 0.0 || p > 1.0) 
	{
	  throw new IllegalArgumentException("Sampling probabiilty must be inside [0, 1].");
	}
  }

  static class ScoredTupleComparator implements Comparator<Tuple>
  {
    public static final ScoredTupleComparator getInstance()
    {
      return _instance;
    }

    private static final ScoredTupleComparator _instance = new ScoredTupleComparator();

    @Override
    public int compare(Tuple o1, Tuple o2)
    {
      try
      {
        ScoredTuple t1 = ScoredTuple.fromIntermediateTuple(o1);
        ScoredTuple t2 = ScoredTuple.fromIntermediateTuple(o2);
        return t1.getScore().compareTo(t2.getScore());
      }
      catch (Throwable e)
      {
        throw new RuntimeException("Cannot compare " + o1 + " and " + o2 + ".", e);
      }
    }
  }

}
