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

/**
 *  Used to do a left outer join of three relations
 *
 *	relation1 - the first relation to join
 *	key1 - the field from the first relation on which to group
 *	relation2 - the second relation to join
 *	key2 - the field from the second relation on which to group
 *	relation3 - the third relation to join
 *	key3 - the field from the third relation on which to group
 *
 */
DEFINE left_outer_join(relation1, key1, relation2, key2, relation3, key3) returns joined {
  DEFINE EmptyBagToNullFields datafu.pig.bags.EmptyBagToNullFields();
  
  cogrouped = COGROUP $relation1 BY $key1, $relation2 BY $key2, $relation3 BY $key3;
  $joined = FOREACH cogrouped GENERATE
    FLATTEN($relation1),
    FLATTEN(EmptyBagToNullFields($relation2)),
    FLATTEN(EmptyBagToNullFields($relation3));
};
