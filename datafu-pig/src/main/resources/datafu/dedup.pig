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
 *  Used to take the most recent row of a table for a given key.
 *
 *	relation - relation to dedup
 *	row_key - field(s) for group by
 *	order_field - the field for ordering (to find the most recent record)
 *
 */
DEFINE dedup(relation, row_key, order_field) returns out {

	DEFINE argmax datafu.org.apache.pig.piggybank.evaluation.ExtremalTupleByNthField('1', 'max');

	with_max_field = FOREACH $relation GENERATE $order_field AS field_for_max, *;
	grouped  = GROUP with_max_field BY $row_key;
	max_only = FOREACH grouped  GENERATE argmax(with_max_field);
	flattened = FOREACH max_only GENERATE FLATTEN($0);
	$out = FOREACH flattened GENERATE $1 .. ;
};
