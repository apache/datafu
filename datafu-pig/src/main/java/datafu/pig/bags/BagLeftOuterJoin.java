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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import datafu.pig.util.AliasableEvalFunc;

/**
 * Performs an in-memory left outer join across multiple bags.
 * 
 * <p>
 * The format for invocation is BagLeftOuterJoin(bag, 'key',....).  
 * This UDF expects that all bags are non-null and that there is a corresponding key for each bag.  
 * The <em>key</em> that is expected is the alias of the key inside of the preceding bag.
 * </p> 
 * 
 * <p>
 * Example:
 * <code>
 * define BagLeftOuterJoin datafu.pig.bags.BagLeftOuterJoin();
 * 
 * -- describe data: 
 * -- data: {bag1: {(key1: chararray,value1: chararray)},bag2: {(key2: chararray,value2: int)}} 
 * 
 * bag_joined = FOREACH data GENERATE BagLeftOuterJoin(bag1, 'key1', bag2, 'key2') as joined;
 * 
 * -- describe bag_joined:
 * -- bag_joined: {joined: {(bag1::key1: chararray, bag1::value1: chararray, bag2::key2: chararray, bag2::value2: int)}} 
 * </code>
 * </p>
 * 
 */
public class BagLeftOuterJoin extends BagJoin
{
  
  public BagLeftOuterJoin() {
    super("left");
  }

}

