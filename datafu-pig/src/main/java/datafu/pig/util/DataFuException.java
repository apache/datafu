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

import java.util.Map;

public class DataFuException extends RuntimeException
{
  private static final long serialVersionUID = 1L;
  private Map<String, Integer> fieldAliases;
  private Object data;
  
  public DataFuException()
  {
    super();
  }
  
  public DataFuException(String message)
  {
    super(message);    
  }
  
  public DataFuException(String message, Throwable cause)
  {
    super(message, cause);    
  }
  
  public DataFuException(Throwable cause)
  {
    super(cause);    
  }

  /**
   * Gets field aliases for a UDF which may be relevant to this exception.
   * 
   * @return field aliases
   */
  public Map<String, Integer> getFieldAliases()
  {
    return fieldAliases;
  }

  /**
   * Gets data relevant to this exception.
   * 
   * @return data the data relevant to this exception
   */
  public Object getData()
  {
    return data;
  }

  /**
   * Sets field aliases for a UDF which may be relevant to this exception.
   * 
   * @param fieldAliases field aliases
   */
  public void setFieldAliases(Map<String, Integer> fieldAliases)
  {
    this.fieldAliases = fieldAliases;
  }

  /**
   * Sets data relevant to this exception.
   * @param data data relevant to this exception
   */
  public void setData(Object data)
  {
    this.data = data;
  }  
  
  @Override
  public String toString()
  {
    String s = getClass().getName();
    String message = getLocalizedMessage();
    
    StringBuilder result = new StringBuilder(s);
    
    if (message != null)
    {
      result.append(": ");
      result.append(message);
    }
    
    if (getFieldAliases() != null)
    {
      result.append("\nAliases:");
      for (String alias : getFieldAliases().keySet())
      {
        result.append("\n");
        result.append(alias != null && alias.length() > 0 ? alias : "???");
      }
    }
    
    if (getData() != null)
    {
      result.append("\nData:");
      result.append("\n");
      result.append(data.toString());
    }
    
    return result.toString();
  }
}
