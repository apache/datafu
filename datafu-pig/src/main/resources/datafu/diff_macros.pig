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

DEFINE diff_macro(diff_macro_old, diff_macro_new, diff_macro_pk, diff_macro_ignored_field) returns diffs {

	DEFINE TupleDiff datafu.pig.util.TupleDiff;
		
	old = 	FOREACH $diff_macro_old GENERATE $diff_macro_pk, TOTUPLE(*) AS original;
	new = 	FOREACH $diff_macro_new GENERATE $diff_macro_pk, TOTUPLE(*) AS original;
	
	join_data = JOIN new BY $diff_macro_pk full, old BY $diff_macro_pk;
		
	join_data = FOREACH join_data GENERATE TupleDiff(old::original, new::original, '$diff_macro_ignored_field') AS tupleDiff, old::original, new::original;
		
	$diffs = FILTER join_data BY tupleDiff IS NOT NULL ;
};