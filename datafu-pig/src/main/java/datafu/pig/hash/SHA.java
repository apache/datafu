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

package datafu.pig.hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import datafu.pig.util.SimpleEvalFunc;

public class SHA extends SimpleEvalFunc<String> {
	private final MessageDigest sha;
	private final String        hex_format;

	public SHA(){
		this("256");
	}
	
	public SHA(String algorithm){
		try {
			sha = MessageDigest.getInstance("SHA-"+algorithm);
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}
        if      (algorithm.equals(  "1")) { hex_format =  "%040x"; }
        else if (algorithm.equals("256")) { hex_format =  "%064x"; }
        else if (algorithm.equals("384")) { hex_format =  "%096x"; }
        else if (algorithm.equals("512")) { hex_format = "%0128x"; }
        else { throw new RuntimeException("Don't know how to format output for SHA-"+algorithm); }
    }

	
	public String call(String value){
        return String.format(hex_format,
                             new BigInteger(1, sha.digest(value.getBytes())));
	}
}
