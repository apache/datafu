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

package datafu.test.pig.hash;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.pig.pigunit.PigTest;
import org.junit.Assert;
import org.testng.annotations.Test;

import datafu.test.pig.PigTests;

public class HashTests extends PigTests
{
  /**
  

  define MD5 datafu.pig.hash.MD5();
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE MD5(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String md5Test;
  
  @Test
  public void md5Test() throws Exception
  {
    PigTest test = createPigTestFromString(md5Test);
    
    writeLinesToFile("input", 
                     "ladsljkasdglk",
                     "lkadsljasgjskdjks",
                     "aladlasdgjks",
                     "has_lo_md5_1065433"
                     );
            
    test.runScript();
        
    assertOutput(test, "data_out",
                 "(d9a82575758bb4978949dc0659205cc6)",
                 "(9ec37f02fae0d8d6a7f4453a62272f1f)",
                 "(cb94139a8b9f3243e68a898ec6bd9b3d)",
                 "(000008e5487b3abae7be88a1d4bad573)");
  }
  
  /**
  
  
  define SHA256 datafu.pig.hash.SHA('256');
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE SHA256(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String sha256Test;
  
  @Test
  public void sha256Test() throws Exception
  {
	  PigTest test = createPigTestFromString(sha256Test);
	  
	  writeLinesToFile("input", 
			  		   "ladsljkasdglk",
                       "lkadsljasgjskdjks",
                       "aladlasdgjks",
                       "has_lo_sha256_11542105");
	  
	  test.runScript();
	  
	  assertOutput(test, "data_out", 
			  "(70ebaf99c4d8ff8860869e50be2d46afbf150b883f66b50a76ee81cdc802242b)",
			  "(f22e3c744a9ade0fa591d28c55392035248b391c9ee4c77ebfeaf6558c8c0dac)",
			  "(49420b42e764178830783d4520aea56b759f325c1d1167f5640ded91f33f3e69)",
			  "(000000aa76ae37c4085105a40d6eb27ef41ab5bed0013002cb6218b1e5fa6315)");
  }
  
  /**
  
  
  define SHA512 datafu.pig.hash.SHA('512');
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE SHA512(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String sha512Test;
  
  @Test
  public void sha512Test() throws Exception
  {
	  PigTest test = createPigTestFromString(sha512Test);
	  
	  writeLinesToFile("input", 
			  		   "ladsljkasdglk",
                       "lkadsljasgjskdjks",
                       "aladlasdgjks",
                       "has_lo_sha512_92333");
	  
	  test.runScript();
	  
	  assertOutput(test, "data_out", 
			  "(f681dbd89dfc9edf00f68107ed81b4b7c89abdf84337921785d13d9189937a43decbc264b5013d396a102b18564c39595732c43d6d4cc99473f6d6d7101ecf87)",
			  "(85c130c8636c052e52a2ca091a92d0bb98ee361adcbeeebbd6af978a593b2486a22ac1e7352c683035cfa28de8eee3402adc6760ad54c5c7eda122c5124766bd)",
			  "(3b82af5c08c9ab70523abf56db244eaa6740fa8c356e3a41bb5225560c0949b14b417c8d56e72cc26d5682400e0420a556e692c41ea82855013e8b7bae5fb0fb)",
			  "(000001f4836cd015a4b2205793484740b65a10626ef032dc8daec65bd369b05b7ecd7493fe263b187fcb356c3e02c05cf4aa5ebbd116305d1ec08d857135c8b2)");
  }
  
  /**
  

  define MD5 datafu.pig.hash.MD5('base64');
  
  data_in = LOAD 'input' as (val:chararray);
  
  data_out = FOREACH data_in GENERATE MD5(val) as val;
  
  STORE data_out INTO 'output';
   */
  @Multiline private String md5Base64Test;
  
  @Test
  public void md5Base64Test() throws Exception
  {
    PigTest test = createPigTestFromString(md5Base64Test);
    
    writeLinesToFile("input", 
                     "ladsljkasdglk",
                     "lkadsljasgjskdjks",
                     "aladlasdgjks",
                     "has_lo_md5_1065433");
            
    test.runScript();
        
    assertOutput(test, "data_out",
                 "(2agldXWLtJeJSdwGWSBcxg==)",
                 "(nsN/Avrg2Nan9EU6YicvHw==)",
                 "(y5QTmoufMkPmiomOxr2bPQ==)",
                 "(AAAI5Uh7Orrnvoih1LrVcw==)");
  }
  

  /**

  define DefaultH    datafu.pig.hash.Hasher();
  define MurmurH32   datafu.pig.hash.Hasher('murmur3-32');
  define MurmurH32A  datafu.pig.hash.Hasher('murmur3-32', '00000000');
  define MurmurH32B  datafu.pig.hash.Hasher('murmur3-32', 'b98b9e85');
  define MurmurH128  datafu.pig.hash.Hasher('murmur3-128');
  define MurmurH128A datafu.pig.hash.Hasher('murmur3-128', '00000000');
  define MurmurH128B datafu.pig.hash.Hasher('murmur3-128', 'b98b9e85');
  --
  define Sip24H      datafu.pig.hash.Hasher('sip24');
  define Sip24HA     datafu.pig.hash.Hasher('sip24', '000102030405060708090a0b0c0d0e0f');
  define Sip24HB     datafu.pig.hash.Hasher('sip24', 'b98b9e856508b355f068d792e1c251c8');
  --
  define SHA1H       datafu.pig.hash.Hasher('sha1');
  define SHA256H     datafu.pig.hash.Hasher('sha256');
  define SHA512H     datafu.pig.hash.Hasher('sha512');
  --
  define MD5H        datafu.pig.hash.Hasher('md5');
  define CRC32       datafu.pig.hash.Hasher('crc32');
  define Adler32     datafu.pig.hash.Hasher('adler32');

  data_in = LOAD 'input' as (val:chararray);

  most_hashes = FOREACH data_in GENERATE
    (MurmurH32A(val)  == DefaultH(val)   ? 'y' : 'n'),
    (MurmurH32A(val)  == MurmurH32(val)  ? 'y' : 'n'),
    (MurmurH128A(val) == MurmurH128(val) ? 'y' : 'n'),
    MurmurH32A(val),  MurmurH32B(val),
    MurmurH128A(val), MurmurH128B(val),
    Sip24HA(val),     Sip24HB(val),
    SHA1H(val),       SHA256H(val),     SHA512H(val),
    MD5H(val),        CRC32(val),       Adler32(val)
    ;

  STORE most_hashes INTO 'output';
   */
  @Multiline private String hasherTest;

  @Test
  public void hasherTest() throws Exception
  {
    PigTest test = createPigTestFromString(hasherTest);

    writeLinesToFile("input",
                     "Of all who give and receive gifts, ",
                     "such as they are wisest. ",
                     "Everywhere they are wisest. They are the magi.",
                     "has_lo_md5_1065433");

    test.runScript();

    assertOutput(test, "most_hashes",
        "(y,y,y,5dd3faff,858e0486,70874abeba0d07a0848bde717968362e,89002495272c9240f228863c9d229253,ad146f93da1f3cbe,f23760b1a2ac49dd,ce2df99fc66f35f8d00abbb3e58a73dcdc46857d,a6114a2f7bad5016cd11b0f74eab219bf629a41184041bb0e3dce16463a39674,73a7c303812965c115f9190ffdff843bf79289e643dc57d1d35865462fe6059daac2cd9209f50547ce63a902dc10f659aa0a4786e338a6e69062d3eeeccee252,b4b0d2cb8da053680556da959b611614,163c13a8,270c9ada)",
        "(y,y,y,f292b84a,07f6261e,cf6c44df722abd72ccc7c485bd259ea3,f9e77d8ad1061430160b0d3a0db7d290,23506548545203c0,2a9323fe502784b0,3db8d76faa5ae6b700db8f083e3162f1ff723edd,156acc8aa21b5110140bb5201245bdfaff99cab38f7a030dc6af8e198687e789,42ae14b43e2e4d75166dec84137c1c157c97a2fbf35e380f4f3b015a0af01a3f868b8ea1566a9b7564fbaef490b4e25614823e811ab43339c14a6d2c2fd0f5d0,8d0e66419d96d4a677f69758a7cf17cf,79710b14,e708e576)",
        "(y,y,y,820c3879,70b0a439,db96091e79c70a61dd328f92a17a657e,45b006d1363e9667081e7667330ff970,b345eac42e6551ad,d9ed1275d9d17639,c9f8b9ec0c9f92a898c81972304ea221ee3b87d3,6b6c1d0e17aa96a8d9dd616e62dc5c00147ba5167bdbaf204b3b3d2a424040fa,30d738452fbd9caaef06f6c7920a02a73797eb7644a361bdf53d154e4f9b2a8fc6a80dc8d3de09706191c76bd87666584fb0150b3c0e8e9a70bf318320771ae3,506a07d334a7fa034550d839671f17f2,348d70a4,a510ed94)",
        "(y,y,y,d3af58ed,134a4b46,301d20747aca51c7b73d4ceaf622b7fa,4d0702752ff1a376475aa12ef72ea832,9081ffb69dfeafc6,82106544171ed2a7,3d08148fe48f0e00486667833fd7b8bdc63412cf,60019a95b6e67b47d7527ff814eeba1001261f54ce14684a14c3ab6f716bc934,7d4f10c90ec6ca2112ae2f92c34e0d8ff557ed3fabdcef6b4b99af72194e6a60f9df311558f6556c04ba220b5c402c4dbb6268158762c6aa91e4e0a6ef13f8ec,000008e5487b3abae7be88a1d4bad573,5c7ff6ae,a1058d3d)");
  }

  /**


  define Murmur32H_R_T    datafu.test.pig.hash.HasherRandForTesting('murmur3-32');
  define Murmur128H_R_T   datafu.test.pig.hash.HasherRandForTesting('murmur3-128');
  define Sip24H_R_T       datafu.test.pig.hash.HasherRandForTesting('sip24');

  define Murmur32H_R_T_2  datafu.test.pig.hash.HasherRandForTesting('murmur3-32'); -- should give same val as R_T1 because we fix the seed
  define Murmur32H_R_1    datafu.pig.hash.HasherRand('murmur3-32');
  define Murmur32H_R_2    datafu.pig.hash.HasherRand('murmur3-32'); -- should not give same val because it's random

  data_in = LOAD 'input' as (val:chararray);

  rand_hashes = FOREACH data_in GENERATE
    (Murmur32H_R_T(val) == Murmur32H_R_T_2(val) ? 'y' : 'n'),
    (Murmur32H_R_T(val) != Murmur32H_R_1(val)   ? 'y' : 'n'),
    (Murmur32H_R_1(val) != Murmur32H_R_2(val)   ? 'y' : 'n'),
    Murmur32H_R_T(val),
    Murmur128H_R_T(val),
    Sip24H_R_T(val)
    ;

  STORE rand_hashes INTO 'output';

   */
  @Multiline private String hasherRandTest;

  @Test
  public void hasherRandTest() throws Exception
  {
    PigTest test = createPigTestFromString(hasherRandTest);

    writeLinesToFile("input",
                     "Of all who give and receive gifts, ",
                     "such as they are wisest. ",
                     "Everywhere they are wisest. They are the magi.");

    test.runScript();

    assertOutput(test, "rand_hashes",
        "(y,y,y,858e0486,89002495272c9240f228863c9d229253,f23760b1a2ac49dd)",
        "(y,y,y,07f6261e,f9e77d8ad1061430160b0d3a0db7d290,2a9323fe502784b0)",
        "(y,y,y,70b0a439,45b006d1363e9667081e7667330ff970,d9ed1275d9d17639)");
  }

  /**


  define GoodH       datafu.pig.hash.Hasher('good-32');
  define BetterH     datafu.pig.hash.Hasher('good-127');

  data_in = LOAD 'input' as (val:chararray);

  vals = FOREACH data_in GENERATE
    GoodH(val) AS h_32,       BetterH(val) AS h_65;

  -- Seed value changes run-to-run, but we can at least ensure the bitness is sound
  good_hashes = FOREACH vals GENERATE
    (SIZE(h_32)*4  >= 32 ? 'y' : 'n') AS got_at_least_32_bits,
    (SIZE(h_65)*4  >= 65 ? 'y' : 'n') AS got_at_least_65_bits;

  STORE good_hashes INTO 'output';
   */
  @Multiline private String hasherGoodTest;

  @Test
  public void hasherGoodTest() throws Exception
  {
    PigTest test = createPigTestFromString(hasherGoodTest);

    writeLinesToFile("input",
                     "Of all who give and receive gifts, ",
                     "such as they are wisest. ",
                     "Everywhere they are wisest. They are the magi.");

    test.runScript();

    assertOutput(test, "good_hashes",
      "(y,y)",
      "(y,y)",
      "(y,y)");
  }

  /*
    Check that seed conversion is correct on your platform
  */
  @Test
  public void hasherSeedConversionTest() throws Exception
  {
    String seed_64 = "f0e0d0c0b0a09080";
    long   seed_lng = datafu.pig.hash.Hasher.longFromHex(seed_64);

    // bits in match bits out
    Assert.assertEquals(seed_64,               Long.toHexString(seed_lng));
    // your bits equal author's bits
    Assert.assertEquals(-1089641583808049024l, seed_lng);

    String seed_32  = "f1e1d1c1";
    int    seed_int = datafu.pig.hash.Hasher.intFromHex(seed_32);

    // bits in match bits out
    Assert.assertEquals(seed_32,     Integer.toHexString(seed_int));
    // your bits equal author's bits
    Assert.assertEquals(-236858943,  seed_int);

  }

  /*
    Check that the random seeds we test with are stable
  */
  @Test
  public void hasherRandGenerationTest() throws Exception
  {
    java.util.Random rg = new java.util.Random(69);
    Assert.assertEquals("b98b9e85", Integer.toHexString(rg.nextInt()));

    rg = new java.util.Random(69);
    Assert.assertEquals("b98b9e856508b355", Long.toHexString(rg.nextLong()));
    Assert.assertEquals("f068d792e1c251c8", Long.toHexString(rg.nextLong()));
  }
  
}
