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
import java.util.Iterator;
import java.util.List;

import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSTaggerME;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * The OpenNLP POSTag UDF tags bags of sequential words with parts of speech and confidence levels using the OpenNLP
 * toolset, and specifically the POSTaggerME class.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define TokenizeME datafu.pig.text.opennlp.TokenizeME('data/en-token.bin');
 * define POSTag datafu.pig.text.opennlp.POSTag('data/en-pos-maxent.bin');
 *
 * -- input:
 * -- (Appetizers during happy hour range from low to high.)
 * infoo = LOAD 'input' AS (text:chararray);
 * --
 * -- ({(Appetizers),(during),(happy),(hour),(range),(from),(low),(to),(high),(.)})
 * tokenized = FOREACH infoo GENERATE TokenizeME(text) AS tokens;
 * --
 * -- output:
 * -- Tuple schema is: (word, tag, confidence)
 * -- ({(Appetizers,NNP,0.3619277937390988),(during,IN,0.7945543860326094),(happy,JJ,0.9888504792754391),
 * -- (hour,NN,0.9427455123502427),(range,NN,0.7335527963654751),(from,IN,0.9911576465589752),(low,JJ,0.9652034031895174),
 * -- (to,IN,0.7005347487371849),(high,JJ,0.8227771746247106),(.,.,0.9900983495480891)})
 * outfoo = FOREACH tokenized GENERATE POSTag(tokens) AS tagged;
 * }
 * </pre>
 */
public class POSTag extends EvalFunc<DataBag>
{
    private POSTaggerME tagger = null;
    private static final String MODEL_FILE = "pos";
    private TupleFactory tf = TupleFactory.getInstance();
    private BagFactory bf = BagFactory.getInstance();
    private String modelPath;

    public POSTag(String modelPath) {
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
        DataBag inputBag = null;

        if(input.size() != 1) {
            throw new IOException();
        }

        inputBag = (DataBag)input.get(0);
        DataBag outBag = bf.newDefaultBag();
        if(this.tagger == null) {
            String loadFile = CachedFile.getFileName(MODEL_FILE, this.modelPath);
            InputStream modelIn = new FileInputStream(loadFile);
            InputStream buffer = new BufferedInputStream(modelIn);
            POSModel model = new POSModel(buffer);
            this.tagger = new POSTaggerME(model);
        }

        // Form an inputString array thing for tagger to act on
        int bagLength = (int)inputBag.size();
        String[] words = new String[bagLength];

        Iterator<Tuple> itr = inputBag.iterator();
        int i = 0;
        while(itr.hasNext()) {
            words[i] = (String)itr.next().get(0);
            i++;
        }

        // Compute tags and their probabilities
        String tags[] = this.tagger.tag(words);
        double probs[] = this.tagger.probs();

        // Build output bag of 3-tuples
        for(int j = 0; j < tags.length; j++) {
            Tuple newTuple = tf.newTuple(3);
            newTuple.set(0, words[j]);
            newTuple.set(1, tags[j]);
            newTuple.set(2, probs[j]);
            outBag.add(newTuple);
        }

        return outBag;
    }

    @Override
    public Schema outputSchema(Schema input)
    {
        try
        {
            Schema.FieldSchema inputFieldSchema = input.getField(0);

            if (inputFieldSchema.type != DataType.BAG)
            {
                throw new RuntimeException("Expected a BAG as input");
            }

            Schema inputBagSchema = inputFieldSchema.schema;

            if(inputBagSchema == null) {
                return null;
            }

            if (inputBagSchema.getField(0).type != DataType.TUPLE)
            {
                throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                        DataType.findTypeName(inputBagSchema.getField(0).type)));
            }

            Schema inputTupleSchema = inputBagSchema.getField(0).schema;

            if (inputTupleSchema.size() != 1)
            {
                throw new RuntimeException("Expected one field for the token data");
            }

            if (inputTupleSchema.getField(0).type != DataType.CHARARRAY)
            {
                throw new RuntimeException(String.format("Expected source to be a CHARARRAY, but instead found %s",
                        DataType.findTypeName(inputTupleSchema.getField(0).type)));
            }

            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema("token",DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("tag",DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema("probability",DataType.DOUBLE));

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
