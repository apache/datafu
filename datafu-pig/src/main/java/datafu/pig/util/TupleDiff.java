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

package datafu.pig.util;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Gets a variable number of arguments - the old tuple, the new tuple, and a list of ignored fields
 *
 * Values are compared by position (zero-based). If a schema exists, the field names will be used for descriptive purposes only.
 *
 * If there are different field names, both will be used, with the format old/new. If there is no schema numbers will be used.
 *
 * The list of ignored fields may be by number or by name. It is only checked at the topmost level. If you want to ignore a field
 * whose name has changed you can use the format oldname/newname, and it will be ignored. To ignore a new field, for example
 * you would use null/newFieldName
 *
 * The following chart shows some sample results. Assume the schema has field names like f0, f1 f2 ... for fields 0, 1, 2, etc.
 *
 * Tuple				Result Without Schema		Result with Schema   
 * ------				----------------------		------------------
 * ((0),)				missing						missing
 * (,(0))				added						added
 * ((0),(0))                                       
 * ((0),(1))			changed 0					changed f0
 * ((0,1),(0,2))		changed 1					changed f1
 * ((0,1),(0,2),1)                                   
 * ((0,1),(0))			changed 1					changed f1/null
 * ((0),(0,1))			changed 1					changed null/f1
 * ((0),(0,1),1)                                   
 * ((0,1),(2,3))		changed 0 1					changed f0 f1
 *
 * The following macro may be useful in calling this UDF if you have only one join field and up to one ignored field:
 *
 * DEFINE diff_macro(diff_macro_old, diff_macro_new, diff_macro_pk, diff_macro_ignored_field) returns diff_macro_diffs
 *    {
 *         DEFINE TupleDiff datafu.pig.util.TupleDiff;

 *         old =     FOREACH $diff_macro_old GENERATE $diff_macro_pk, TOTUPLE(*) AS original;
 *         new =     FOREACH $diff_macro_new GENERATE $diff_macro_pk, TOTUPLE(*) AS original;
 *
 *         join_data = JOIN new BY $diff_macro_pk full, old BY $diff_macro_pk;
 *
 *         join_data = FOREACH join_data GENERATE TupleDiff(old::original, new::original, '$diff_macro_ignored_field') AS tupleDiff, old::original, new::original;
 *
 *         $diff_macro_diffs = FILTER join_data BY tupleDiff IS NOT NULL ;
 *     };
 *
 **/
public class TupleDiff extends EvalFunc<String> {

	public final String added;
	public final String missing;
	public final String changed;
	public final String separator;
	public final String fieldNameChangeSeparator;
	public final String tupleStart;
	public final String tupleEnd;

	public TupleDiff() {
		this("added", "missing", "changed", " ", "/", "(", ")");
	}

	public TupleDiff(String added, String missing, String changed, String separator, String fieldNameChangeSeparator,
			String tupleStart, String tupleEnd) {
		this.added = added;
		this.missing = missing;
		this.changed = changed;
		this.separator = separator;
		this.fieldNameChangeSeparator = fieldNameChangeSeparator;
		this.tupleStart = tupleStart;
		this.tupleEnd = tupleEnd;
	}

	@Override
	public String exec(Tuple tuple) throws IOException {
		Schema inputSchema = this.getInputSchema();

		FieldSchema schema1 = inputSchema != null ? inputSchema.getField(0) : null;
		FieldSchema schema2 = inputSchema != null ? inputSchema.getField(1) : null;

		Set<String> ignoredFields = new HashSet<String>();

		// only top-level fields can be ignored
		for (int i = 2; i < tuple.size(); i++) {
			ignoredFields.add((String) tuple.get(i));
		}

		StringBuilder sb = new StringBuilder();

		boolean addedOrMissing = compare(tuple.get(0), tuple.get(1), schema1, schema2, 0, sb, null, ignoredFields);

		if (sb.length() == 0) {
			return null;
		}

		String result = sb.toString();

		if (addedOrMissing) {
			return result;
		}

		// if there were ignored fields, we will only know that our tuples have no diff at the very end
		return this.changed + result;
	}

	private boolean compare(Object oldField, Object newField, FieldSchema oldSchema, FieldSchema newSchema,
			int fieldNum, StringBuilder resultBuilder, StringBuilder prefixBuilder, Set<String> ignoredFields)
			throws ExecException, FrontendException {
		boolean topLevel = prefixBuilder == null;

		String alias = topLevel ? "" : getFieldAliasOrNumber(oldSchema, newSchema, fieldNum);

		// we are not in the 2nd level and this field is meant to be ignored
		if (topLevel || (prefixBuilder.length() != 0) || !ignoredFields.contains(alias)) {
			if (oldField == null) {
				if (newField != null) {
					if (topLevel) {
						resultBuilder.append(this.added);
						return true;
					} else {
						resultBuilder.append(this.separator).append(prefixBuilder.toString()).append(alias);
					}
				}
			} else if (newField == null) {
				if (topLevel) {
					resultBuilder.append(this.missing);
					return true;
				} else {
					resultBuilder.append(this.separator).append(prefixBuilder.toString()).append(alias);
				}
			} else if (!newField.equals(oldField)) {

				// the top level is always a tuple, regardless of whether we have a schema or not
				if (topLevel || ((oldSchema != null) && (DataType.TUPLE == oldSchema.type))) {
					Tuple oldTuple = (Tuple) oldField;

					if (!(newField instanceof Tuple)) {
						resultBuilder.append(this.separator).append(prefixBuilder.toString()).append(alias);
					} else {

						// drill down to show the differences within the tuple
						Tuple newTuple = (Tuple) newField;

						int maxNumOfFields = Math.max(oldTuple.size(), newTuple.size());

						for (int innerFieldNum = 0; innerFieldNum < maxNumOfFields; innerFieldNum++) {

							int lengthBeforeCall = resultBuilder.length();

							compare(getField(oldTuple, innerFieldNum), getField(newTuple, innerFieldNum),
									getFieldSchema(oldSchema, innerFieldNum), getFieldSchema(newSchema, innerFieldNum),
									innerFieldNum, resultBuilder, topLevel ? new StringBuilder() : new StringBuilder(
											prefixBuilder).append(alias).append(this.tupleStart), ignoredFields);

							if (!topLevel && (resultBuilder.length() > lengthBeforeCall)) {
								resultBuilder.append(this.tupleEnd);
							}

						}
					}
				} else {
					resultBuilder.append(this.separator).append(prefixBuilder.toString()).append(alias);
				}
			}
		}

		return false;
	}

	private String getFieldAliasOrNumber(FieldSchema oldSchema, FieldSchema newSchema, int fieldNum) {
		String oldAlias = oldSchema != null ? oldSchema.alias : null;
		String newAlias = newSchema != null ? newSchema.alias : null;

		if ((oldAlias == null) && (newAlias == null)) {
			return String.valueOf(fieldNum);
		} else if ((oldAlias != null) && oldAlias.equals(newAlias)) {
			return oldAlias;
		}

		return oldAlias + this.fieldNameChangeSeparator + newAlias;
	}

	private Object getField(Tuple tuple, int fieldNum) throws ExecException {
		return tuple.size() < (fieldNum + 1) ? null : tuple.get(fieldNum);
	}

	private FieldSchema getFieldSchema(FieldSchema fieldSchema, int fieldNum) throws ExecException, FrontendException {
		if (fieldSchema == null) {
			return null;
		}

		Schema schema = fieldSchema.schema;

		return schema.size() < (fieldNum + 1) ? null : schema.getField(fieldNum);
	}

	@Override
	public Schema outputSchema(Schema input) {
		if (input.size() < 2) {
			throw new RuntimeException("Expected input to have at least 2 fields, but has " + input.size());
		}

		return new Schema(new FieldSchema("tuplediff", DataType.CHARARRAY));
	}
}