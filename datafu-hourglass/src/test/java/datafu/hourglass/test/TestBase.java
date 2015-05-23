/**
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

package datafu.hourglass.test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.log4j.Logger;

public class TestBase
{  
  private FileSystem _fileSystem;
  protected String _confDir;
  
  public TestBase() throws IOException 
  {
      _confDir = System.getProperty("confdir");
  }
  
  public void beforeClass() throws Exception
  {
    // make sure the log folder exists or it will fail
    new File("test-logs").mkdirs();    
    System.setProperty("hadoop.log.dir", "test-logs");
    
    _fileSystem = FileSystem.get(new JobConf());
  }
  
  public void afterClass() throws Exception
  {
  }
  
  /**
   * Returns the Filesystem in use.
   *
   * TestCases should use this Filesystem as it
   * is properly configured with the workingDir for relative PATHs.
   *
   * @return the filesystem used by Hadoop.
   */
  protected FileSystem getFileSystem() {
    return _fileSystem;
  }

  /**
   * Returns a job configuration preconfigured to run against the Hadoop
   * managed by the testcase.
   * @return configuration that works on the testcase Hadoop instance
   */
  protected JobConf createJobConf() 
  {
    return new JobConf();
  }
  
  /**
   * Stores the configuration in the properties as "test.conf" so it can 
   * be used by the job.  This property is a special test hook to enable
   * testing.
   * 
   * @param props
   * @throws IOException
   */
  protected void storeTestConf(Properties props) throws IOException
  {
    ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataStream = new DataOutputStream(arrayOutputStream);    
    createJobConf().write(dataStream);  
    dataStream.flush();
    props.setProperty("test.conf", new String(Base64.encodeBase64(arrayOutputStream.toByteArray())));
  }
  
  /**
   * Creates properties object for testing.  This contains test configuration that will
   * be extracted by the Hadoop jobs so the test cluster is used.
   * 
   * @return properties
   * @throws IOException
   */
  protected Properties newTestProperties() throws IOException
  {
    Properties props = new Properties();
    storeTestConf(props);  
    return props;
  }
  
  protected String getDataPath()
  {
    if (System.getProperty("hourglass.data.dir") != null)
    {
      return System.getProperty("hourglass.data.dir");
    }
    else
    {
      return new File(System.getProperty("user.dir"), "data").getAbsolutePath();
    }  
  }
}
