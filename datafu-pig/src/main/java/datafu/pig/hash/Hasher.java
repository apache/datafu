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

import com.google.common.hash.Hashing;
import com.google.common.hash.HashFunction;

import datafu.pig.util.SimpleEvalFunc;

/**
 *
 * Computes a hash value of a string and outputs it in hex.
 *
 * The default constructor produces a fast 32-bit non-cryptographic-strength
 * hash ('murmur3-32') that is reasonable if all you need is good
 * mixing. Additional constructors are available to choose alternative hash
 * functions:
 *
 * <ul>
 * <li><code>'murmur3-32', [optional seed]</code> or <code>'murmur3-128',
 *   [optional seed]</code>: Returns a <a
 *   href="https://code.google.com/p/smhasher/">murmur3 hash</a> of the given
 *   length. Murmur3 is fast, with has exceptionally good statistical
 *   properties; it's a good choice if all you need is good mixing of the
 *   inputs. It is <em>not</em> cryptographically secure; that is, given an
 *   output value from murmur3, there are efficient algorithms to find an input
 *   yielding the same output value. Supply the seed as a string that
 *   <a href="http://docs.oracle.com/javase/7/docs/api/java/lang/Integer.html#decode(java.lang.String)">Integer.decode</a>
 *   can handle. Examples: <code>datafu.pig.hash.Hasher('murmur3-32', '0x56789abc');</code> or <code>datafu.pig.hash.Hasher('murmur3-32', '-12345678');</code>.</li>
 * <li><code>'sip24', [optional seed]</code>: Returns a <a href="https://131002.net/siphash/">64-bit
 *   SipHash-2-4</a> hash. SipHash is competitive in performance with Murmur3,
 *   and is simpler and faster than the cryptographic algorithms below. When
 *   used with a seed, it can be considered cryptographically secure: given
 *   the output from a sip24 instance but not the seed used, we cannot
 *   efficiently craft a message yielding the same output from that instance. To
 *   supply a seed, pass in a 32-character string representing the seed in
 *   hexadecimal. If none is given, k = '00010203&hellip;0e0f' is used.</li>
 * <li><code>'adler32'</code>: Returns an Adler-32 checksum (32 hash bits) by delegating to Java's Adler32 Checksum.</li>
 * <li><code>'crc32'</code>:   Returns a CRC-32 checksum (32 hash bits) by delegating to Java's CRC32 Checksum.</li>
 * <li><code>'md5'</code>:     Returns an MD5 hash (128 hash bits) using Java's MD5 MessageDigest.</li>
 * <li><code>'sha1'</code>:    Returns a SHA-1 hash (160 hash bits) using Java's SHA-1 MessageDigest.</li>
 * <li><code>'sha256'</code>:  Returns a SHA-256 hash (256 hash bits) using Java's SHA-256 MessageDigest.</li>
 * <li><code>'sha512'</code>:  Returns a SHA-512 hash (160 hash bits) using Java's SHA-512 MessageDigest.</li>
 * <li><code>'good-{integer number of bits}'</code>: Returns a general-purpose,
 *   <i>non-cryptographic-strength</i>, streaming hash function that produces
 *   hash codes of length at least minimumBits. Users without specific'
 *   compatibility requirements and who do not persist the hash codes are
 *   encouraged to choose this hash function. (Cryptographers, like dieticians
 *   and fashionistas, occasionally realize that We've Been Doing it Wrong
 *   This Whole Time. Using 'good-*' lets you track What the Experts From
 *   (Milan|NIH|IEEE) Say To (Wear|Eat|Hash With) this Fall.) Expect values
 *   returned by this hasher to change run-to-run.</li>
 * </ul>
 *
 */
public class Hasher extends SimpleEvalFunc<String>
{
  protected     HashFunction hash_func = null;

  private static final String HASH_NAMES = "'murmur3-32' (with optional seed); 'murmur3-128' (with optional seed); 'sip24' (with optional seed); 'crc32', 'adler32', 'md5'; 'sha1'; 'sha256'; 'sha512';  or 'good-{number of bits}'.";
  protected static final String SEEDED_HASH_NAMES = "'murmur3-32' (with optional seed); 'murmur3-128' (with optional seed); 'sip24' (with optional seed)";

  /**
   * Generates hash values according to murmur3-32, a non-cryptographic-strength
   * hash function with good mixing.
   *
   * @throws IllegalArgumentException, RuntimeException
   */
  public Hasher() throws IllegalArgumentException, RuntimeException
  {
    this("murmur3-32");
  }

  /**
   * Generates hash values according to the hash function given by algorithm.
   *
   * See the Hasher class docs for a list of algorithms and guidance on selection.
   *
   * @param algorithm
   * @throws IllegalArgumentException, RuntimeException
   * @see    Hasher#makeHashFunc(String algorithm)
   *
   */
  public Hasher(String algorithm) throws IllegalArgumentException, RuntimeException
  {
    makeHashFunc(algorithm);
  }

  /**
   * Generates hash values according to the hash function given by algorithm,
   * with initial seed given by the seed.
   *
   * See the Hasher class docs for a list of algorithms and guidance on selection.
   *
   * @param  algorithm
   * @param  seed
   * @throws IllegalArgumentException, RuntimeException
   * @see    Hasher#makeHashFunc(String algorithm, String seed)
   *
   */
  public Hasher(String algorithm, String seed) throws IllegalArgumentException, RuntimeException
  {
    makeHashFunc(algorithm, seed);
  }

  /**
   * Returns the HashFunction named by algorithm
   *
   * See the Hasher class docs for a list of algorithms and guidance on selection.
   *
   * @param algorithm
   * @throws IllegalArgumentException, RuntimeException
   */
  private void makeHashFunc(String algorithm) throws IllegalArgumentException, RuntimeException
  {
    if (hash_func != null) { throw new RuntimeException("The hash function should only be set once per instance"); }

    if      (algorithm.startsWith("good-")) {
      int bits = Integer.parseInt(algorithm.substring(5));
      hash_func = Hashing.goodFastHash(bits);
    }
    else if (algorithm.equals("murmur3-32")) { hash_func = Hashing.murmur3_32();  }
    else if (algorithm.equals("murmur3-128")){ hash_func = Hashing.murmur3_128(); }
    else if (algorithm.equals("sip24"))      { hash_func = Hashing.sipHash24();   }
    else if (algorithm.equals("sha1"))       { hash_func = Hashing.sha1();        }
    else if (algorithm.equals("sha256"))     { hash_func = Hashing.sha256();      }
    else if (algorithm.equals("sha512"))     { hash_func = Hashing.sha512();      }
    else if (algorithm.equals("md5"))        { hash_func = Hashing.md5();         }
    else if (algorithm.equals("adler32"))    { hash_func = Hashing.adler32();     }
    else if (algorithm.equals("crc32"))      { hash_func = Hashing.crc32();       }
    else { throw new IllegalArgumentException("No hash function found for algorithm "+algorithm+". Allowed values include "+HASH_NAMES); }
  }

  /**
   * Returns the HashFunction named by algorithm, with initial seed given by the
   * seed.
   *
   * See the Hasher class docs for a list of algorithms and guidance on selection.
   *
   * The seed is interpreted as follows:
   *
   * <ul>
   * <li>With algorithm 'murmur3-32' or 'murmur3-128', supply a 32-bit
   *   number as a string of exactly 8 hexadecimal digits.</li>
   * <li>With algorithm 'sip24' supply a 128-bit number as a string of exactly
   *   32 hexadecimal digits.</li>
   * </ul>
   *
   * @param algorithm
   * @param seed
   * @throws IllegalArgumentException, RuntimeException
   */
  protected void makeHashFunc(String algorithm, String seed) throws IllegalArgumentException, RuntimeException
  {
    try {
      if (algorithm.equals("murmur3-32")) {
        if (seed.length() != 8) { throw new IllegalArgumentException("Seed for "+algorithm+" must be an 8-character string representing a 32-bit unsigned number in hexadecimal."); }
        int seedint = Hasher.intFromHex(seed);
        hash_func = Hashing.murmur3_32(seedint);
      }
      else if (algorithm.equals("murmur3-128")) {
        if (seed.length() != 8) { throw new IllegalArgumentException("Seed for "+algorithm+" must be an 8-character string representing a 32-bit unsigned number in hexadecimal."); }
        int seedint = Hasher.intFromHex(seed);
        hash_func = Hashing.murmur3_128(seedint);
      }
      else if (algorithm.equals("sip24")){
        if (seed.length() != 32){ throw new IllegalArgumentException("Seed for "+algorithm+" must be a 32-character string representing a 128-bit unsigned number in hexadecimal."); }
        long k0 = Hasher.longFromHex(seed.substring( 0,16));
        long k1 = Hasher.longFromHex(seed.substring(16,32));
        hash_func = Hashing.sipHash24(k0, k1);
      }
      else { throw new IllegalArgumentException("No hash function found for algorithm "+algorithm+" with a seed. Allowed values include "+SEEDED_HASH_NAMES); }
    }
    catch (NumberFormatException err) {
      throw new RuntimeException(err);
    }
  }

  /*
   * Convert the given unsigned hex string to a long value.  Values higher than
   * MAX_LONG become negative. A value higher than 2^64 is undefined behavior.
   * For example, f0e0d0c0b0a09080 becomes -1089641583808049024l.
   */
  public static long longFromHex(String hex_str) {
    return ( new BigInteger(hex_str, 16) ).longValue();
  }

  /*
   * Convert the given unsigned hex string to an int value.  Values higher than
   * MAX_INT become negative. A value higher than 2^32 is undefined behavior.
   * For example, f1e1d1c1 becomes -236858943.
   */
  public static int intFromHex(String hex_str) {
    return ( new BigInteger(hex_str, 16) ).intValue();
  }

  public String call(String val)
  {
    return hash_func.hashBytes(val.getBytes()).toString();
  }
}
