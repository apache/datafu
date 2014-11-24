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

package datafu.pig.bags;

import datafu.pig.util.AliasableEvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.*;

/**
 * Performs an in-memory join across multiple bags.
 * 
 * <p>
 * The format for invocation is BagJoin(bag, 'key',....).
 * This UDF expects that all bags are non-null and that there is a corresponding key for each bag.  
 * The <em>key</em> that is expected is the alias of the key inside of the preceding bag.  By default, an 'inner'
 * join is performed.  You can also perform 'left' or 'full' outer joins by specifying 'left' or 'full' in the
 * definition.
 * </p> 
 * 
 * <p>
 * Example:
 * <code>
 * define BagJoin datafu.pig.bags.BagJoin(); -- inner join
 * 
 * -- describe data: 
 * -- data: {bag1: {(key1: chararray,value1: chararray)},bag2: {(key2: chararray,value2: int)}} 
 * 
 * bag_joined = FOREACH data GENERATE BagJoin(bag1, 'key1', bag2, 'key2') as joined;
 * 
 * -- describe bag_joined:
 * -- bag_joined: {joined: {(bag1::key1: chararray, bag1::value1: chararray, bag2::key2: chararray, bag2::value2: int)}} 
 * </code>
 * </p>
 * 
 */
public class BagJoin extends AliasableEvalFunc<DataBag>
{
  private static final String BAG_NAMES_PROPERTY = "BagFullOuterJoin_BAG_NAMES";
  private static final String BAG_NAME_TO_JOIN_PREFIX_PROPERTY = "BagFullOuterJoin_BAG_NAME_TO_JOIN_PREFIX";
  private static final String BAG_NAME_TO_SIZE_PROPERTY = "BagFullOuterJoin_BAG_NAME_TO_SIZE_PROPERTY";

  private final JoinType joinType;

  ArrayList<String> bagNames;
  Map<String, String> bagNameToJoinKeyPrefix;
  Map<String, Integer> bagNameToSize;


  public enum JoinType { INNER,LEFT,FULL }

  public BagJoin() {
    this("inner");
  }

  public BagJoin(String joinType) {
      if ("left".equals(joinType.toLowerCase())) {
          this.joinType = JoinType.LEFT;
      } else if ("full".equals(joinType.toLowerCase())) {
          this.joinType = JoinType.FULL;
      } else if ("inner".equals(joinType.toLowerCase())) {
          this.joinType = JoinType.INNER;
    } else {
          throw new IllegalArgumentException("Invalid constructor argument.  Valid values are 'left' or 'full', found: " + joinType);
    }
  }

  @SuppressWarnings("unchecked")
  private void retrieveContextValues()
  {
    Properties properties = getInstanceProperties();
    bagNames = (ArrayList<String>) properties.get(BAG_NAMES_PROPERTY);
    bagNameToJoinKeyPrefix = (Map<String, String>) properties.get(BAG_NAME_TO_JOIN_PREFIX_PROPERTY);
    bagNameToSize = (Map<String, Integer>) properties.get(BAG_NAME_TO_SIZE_PROPERTY);
  }

  class JoinCollector
  {
    private final JoinType joinType;

    Set<Object> keys;
    HashMap<Object, List<Tuple>> data;
    int tupleSize = 0;

    public JoinCollector(JoinType joinType) {
        this.joinType = joinType;
    }

    public void printData() throws ExecException {
      for (Entry<Object, List<Tuple>> entry : data.entrySet()) {
        System.out.println(entry.getKey());
        for (Tuple t : entry.getValue()) {
          System.out.println("\t"+t.toDelimitedString(", "));
        }
      }
    }

    public HashMap<Object, List<Tuple>> join(Iterable<Tuple> tuples, String keyName, int tupleSize) throws ExecException {

      // if this is the first list of tuples, then just add them all
      if (this.data == null) {
        this.data = new HashMap<Object, List<Tuple>>();
        this.keys = new HashSet<Object>();
        for (Tuple tuple : tuples) {
          Object key = getObject(tuple, keyName);
          if (!this.data.containsKey(key)) {
            this.data.put(key, new ArrayList<Tuple>());

            // add this key to our set of keys for convenience
            this.keys.add(key);
          }
          this.data.get(key).add(tuple);
        }

        this.tupleSize = tupleSize;
        return data;
      }

      // otherwise, join
      HashMap<Object, List<Tuple>> joinedData = new HashMap<Object, List<Tuple>>(this.data.size());
      Set<Object> joinedKeys = new HashSet<Object>();
      for (Tuple tuple : tuples) {
        Object key = getObject(tuple, keyName);
        if (data.containsKey(key)) {
            if (!joinedData.containsKey(key)) {
                joinedData.put(key, new ArrayList<Tuple>());
                joinedKeys.add(key);
            }
            for (Tuple t : this.data.get(key)) {
                Tuple joinedTuple = TupleFactory.getInstance().newTuple();
                for (Object o : t.getAll()) {
                    joinedTuple.append(o);
                }
                for (Object o : tuple.getAll()) {
                    joinedTuple.append(o);
                }
                joinedData.get(key).add(joinedTuple);
            }

            // remove from the previous key set for dealing with left join
            this.keys.remove(key);

        } else if (this.joinType == JoinType.FULL) {
            if (!joinedData.containsKey(key)) {
                joinedData.put(key, new ArrayList<Tuple>());
                joinedKeys.add(key);
            }
            Tuple nullTuple = TupleFactory.getInstance().newTuple(this.tupleSize);
            for (Object o : tuple.getAll()) {
                nullTuple.append(o);
            }
            joinedData.get(key).add(nullTuple);
        }
      }

      // process the keys that did not join
      if (this.joinType == JoinType.LEFT || this.joinType == JoinType.FULL) {
          for (Object key : this.keys) {
              // join with a null tuple
              if (!joinedData.containsKey(key)) {
                  joinedData.put(key, new ArrayList<Tuple>());
                  joinedKeys.add(key);
              }
              for (Tuple t : this.data.get(key)) {
                  Tuple joinedTuple = TupleFactory.getInstance().newTuple();
                  for (Object o : t.getAll()) {
                      joinedTuple.append(o);
                  }
                  for (int i = 0; i < tupleSize; i++) {
                      joinedTuple.append(null);
                  }
                  joinedData.get(key).add(joinedTuple);
              }
          }
      }

      this.data = joinedData;
      this.keys = joinedKeys;
      this.tupleSize += tupleSize;

      return this.data;
    }

    public HashMap<Object, List<Tuple>> getJoinData() {
        return this.data;
    }

  }

  @Override
  public DataBag exec(Tuple input) throws IOException
  {
    retrieveContextValues();

    HashMap<String,String> joinKeyNames = new HashMap<String,String>();
    for (int i = 1; i < input.size(); i += 2) {
      joinKeyNames.put(bagNames.get(i / 2), (String) input.get(i));
    }

    JoinCollector collector = new JoinCollector(this.joinType);

    for (String bagName: bagNames) {
        DataBag bag = getBag(input, bagName);
        String joinKeyName = getPrefixedAliasName(bagNameToJoinKeyPrefix.get(bagName), joinKeyNames.get(bagName));
        int tupleSize = bagNameToSize.get(bagName);
        if (bag == null) throw new IOException("Error in instance: "+getInstanceName()
                + " with properties: " + getInstanceProperties()
                + " and tuple: " + input.toDelimitedString(", ")
                + " -- Expected bag, got null");

        collector.join(bag, joinKeyName, tupleSize);
    }

    // assemble output bag
    DataBag outputBag = BagFactory.getInstance().newDefaultBag();
    for (List<Tuple> tuples : collector.getJoinData().values()) {
      for (Tuple tuple : tuples) {
        outputBag.add(tuple);
      }
    }

    return outputBag;
  }

  @Override
  public Schema getOutputSchema(Schema input)
  {
    ArrayList<String> bagNames = new ArrayList<String>(input.size() / 2);
    Map<String, String> bagNameToJoinPrefix = new HashMap<String, String>(input.size() / 2);
    Map<String, Integer> bagNameToSize = new HashMap<String, Integer>(input.size() / 2);
    Schema outputSchema = null;
    Schema bagSchema = new Schema();
    try {
      int i = 0;
      // all even fields should be bags, odd fields are key names
      String bagName = null;
      String tupleName = null;
      for (FieldSchema outerField : input.getFields()) {
        if (i++ % 2 == 1)
          continue;
        bagName = outerField.alias;
        bagNames.add(bagName);
        if (bagName == null)
          bagName = "null";
        if (outerField.schema == null)
          throw new RuntimeException("Expected input format of (bag, 'field') pairs. "
              +"Did not receive a bag at index: "+i+", alias: "+bagName+". "
              +"Instead received type: "+DataType.findTypeName(outerField.type)+" in schema:"+input.toString());
        FieldSchema tupleField = outerField.schema.getField(0);
        tupleName = tupleField.alias;
        bagNameToJoinPrefix.put(bagName, getPrefixedAliasName(outerField.alias, tupleName));
        if (tupleField.schema == null) {
          log.error(String.format("could not get schema for inner tuple %s in bag %s", tupleName, bagName));
        } else {
          bagNameToSize.put(bagName, tupleField.schema.size());
          for (FieldSchema innerField : tupleField.schema.getFields()) {
            String innerFieldName = innerField.alias;
            if (innerFieldName == null)
              innerFieldName = "null";
            String outputFieldName = bagName + "::" + innerFieldName;
            if (innerField.schema != null) {
                bagSchema.add(new FieldSchema(outputFieldName, innerField.schema, innerField.type));
            } else {
                bagSchema.add(new FieldSchema(outputFieldName, innerField.type));
            }
          }
        }
      }
      outputSchema = new Schema(new Schema.FieldSchema(
                getSchemaName(this.getClass().getName().toLowerCase(), input),
                bagSchema,
                DataType.BAG));
      log.debug("output schema: "+outputSchema.toString());
    } catch (FrontendException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    Properties properties = getInstanceProperties();
    properties.put(BAG_NAMES_PROPERTY, bagNames);
    properties.put(BAG_NAME_TO_JOIN_PREFIX_PROPERTY, bagNameToJoinPrefix);
    properties.put(BAG_NAME_TO_SIZE_PROPERTY, bagNameToSize);
    return outputSchema;
  }

}

