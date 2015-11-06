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
package datafu.pig.text.opennlp;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * The OpenNLP Tokenizers segment an input character sequence into tokens using the OpenNLP TokenizeME class, which is
 * a probabilistic, 'maximum entropy' classifier.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define TokenizeME datafu.pig.text.opennlp.TokenizeME('data/en-token.bin');
 *
 * -- input:
 * -- ("I believe the Masons have infiltrated the Apache PMC.")
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- ({(I),(believe),(the),(Masons),(have),(infiltrated),(the),(Apache),(PMC),(.)})
 * outfoo = FOREACH infoo GENERATE TokenizeME(text) as tokens;
 * }
 * </pre>
 */



public class TokenizeME extends EvalFunc<DataBag>
{
    private TokenizerME tokenizer = null;
    private static final String MODEL_FILE = "tokens";
    private TupleFactory tf = TupleFactory.getInstance();
    private BagFactory bf = BagFactory.getInstance();
    private String modelPath;

    public TokenizeME(String modelPath) {
        this.modelPath = modelPath;
    }

    @Override
    public List<String> getCacheFiles() {
        List<String> list = new ArrayList<String>(1);
        list.add(this.modelPath + "#" + MODEL_FILE);
        return list;
    }

    // Enable multiple languages by specifying the model path. See http://text.sourceforge.net/models-1.5/
    public DataBag exec(Tuple input) throws IOException
    {
        if(input.size() != 1) {
            throw new IOException();
        }

        String inputString = input.get(0).toString();
        if(inputString == null || inputString == "") {
            return null;
        }
        DataBag outBag = bf.newDefaultBag();
        if(this.tokenizer == null) {
            String loadFile = CachedFile.getFileName(MODEL_FILE, this.modelPath);;
            InputStream file = new FileInputStream(loadFile);
            InputStream buffer = new BufferedInputStream(file);
            TokenizerModel model = new TokenizerModel(buffer);
            this.tokenizer = new TokenizerME(model);
        }
        String tokens[] = this.tokenizer.tokenize(inputString);
        for(String token : tokens) {
            Tuple outTuple = tf.newTuple(token);
            outBag.add(outTuple);
        }
        return outBag;
    }

    @Override
    public Schema outputSchema(Schema input)
    {
        try
        {
            Schema.FieldSchema inputFieldSchema = input.getField(0);

            if (inputFieldSchema.type != DataType.CHARARRAY)
            {
                throw new RuntimeException("Expected a CHARARRAY as input, but got a " + inputFieldSchema.toString());
            }

            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("token",DataType.CHARARRAY));

            return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
                    .getName()
                    .toLowerCase(), input),
                    tupleSchema,
                    DataType.BAG));
        }
        catch (FrontendException e)
        {
            throw new RuntimeException(e);
        }
    }
}
