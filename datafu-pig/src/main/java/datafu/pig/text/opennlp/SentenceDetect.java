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

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * The OpenNLP SentenceDectectors segment an input paragraph into sentences.
 * <p>
 * Example:
 * <pre>
 * {@code
 * define SentenceDetect datafu.pig.text.opennlp.SentenceDetect('data/en-sent.bin');
 *
 * -- input:
 * -- ("I believe the Masons have infiltrated the Apache PMC. I believe laser beams control cat brains.")
 * infoo = LOAD 'input' AS (text:chararray);

 * -- output:
 * -- ({(I believe the Masons have infiltrated the Apache PMC.)(I believe laser beams control cat brains.)})
 * outfoo = FOREACH infoo GENERATE SentenceDetect(text) as sentences;
 * }
 * </pre>
 */
public class SentenceDetect extends EvalFunc<DataBag>
{
    private SentenceDetectorME sdetector = null;
    private static final String MODEL_FILE = "sentences";
    private TupleFactory tf = TupleFactory.getInstance();
    private BagFactory bf = BagFactory.getInstance();
    private String modelPath = null;

    public SentenceDetect(String modelPath) {
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
        if(inputString == null || inputString.equals("")) {
            return null;
        }
        DataBag outBag = bf.newDefaultBag();
        if(sdetector == null) {
            String loadFile = CachedFile.getFileName(MODEL_FILE, this.modelPath);
            InputStream is = new FileInputStream(loadFile);
            InputStream buffer = new BufferedInputStream(is);
            SentenceModel model = new SentenceModel(buffer);
            this.sdetector = new SentenceDetectorME(model);
        }
        String sentences[] = this.sdetector.sentDetect(inputString);
        for(String sentence : sentences) {
            Tuple outTuple = tf.newTuple(sentence);
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
            tupleSchema.add(new Schema.FieldSchema("sentence",DataType.CHARARRAY));

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
