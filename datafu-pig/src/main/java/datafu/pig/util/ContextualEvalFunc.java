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

import java.util.Properties;

import org.apache.pig.EvalFunc;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * An abstract class which enables UDFs to store instance properties
 * on the front end which will be available on the back end.
 *
 * For example, you can override the onSchemaReady hook method to set properties
 * at front-end (i.e. at launch time) which will be available when exec() is
 * called (on the workers themselves.
 * 
 * @param <T>
 */
public abstract class ContextualEvalFunc<T> extends EvalFunc<T>
{
  private String instanceName;
  
  @Override
  public void setUDFContextSignature(String signature) {
    setInstanceName(signature);
  }
  
  /**
   * Helper method to return the context properties for this class
   * 
   * @return context properties
   */
  protected Properties getContextProperties() {
    UDFContext context = UDFContext.getUDFContext();
    Properties properties = context.getUDFProperties(this.getClass());
    return properties;
  }
  
  /**
   * Helper method to return the context properties for this instance of this class
   * 
   * @return instances properties
   */
  protected Properties getInstanceProperties() {
    Properties contextProperties = getContextProperties();
    if (!contextProperties.containsKey(getInstanceName())) {
      contextProperties.put(getInstanceName(), new Properties());
    }
    return (Properties)contextProperties.get(getInstanceName());
  }
  
  /**
   * 
   * @return the name of this instance corresponding to the UDF Context Signature
   * @see #setUDFContextSignature(String)
   */
  protected String getInstanceName() {
    if (instanceName == null) {
      throw new RuntimeException("Instance name is null.  This should not happen unless UDFContextSignature was not set.");
    }
    return instanceName;
  }
  
  private void setInstanceName(String instanceName) {
    this.instanceName = instanceName;
  }

  /**
   * Hook method, called once the input and output schema are prepared.
   *
   * Subclasses may override to set properties on the front end (i.e. at script
   * run time) that may be played with later (e.g. at execution time).
   *
   * Child classes must (a) call super.onReady(in_schema, out_schema) so
   * that the hook chains, and (b) not mess with the schema.
   *
   * @param in_schema input schema
   * @param out_schema output schema
   */
  protected void onReady(Schema in_schema, Schema out_schema)
  {
    /* ze goggles! zey do nussing! */
  }

  /**
   * Override outputSchema only to add the onSchemaReady hook method. In all
   * other respects delegates to the superclass outputSchema preparation.
   *
   * @param in_schema input schema
   * @return call to super.outputSchema
   */
  @Override
  public Schema outputSchema(Schema in_schema)
  {
    Schema out_schema = super.outputSchema(in_schema);
    onReady(in_schema, out_schema);
    return out_schema;
  }
}
