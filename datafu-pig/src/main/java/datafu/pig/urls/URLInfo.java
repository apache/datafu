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

package datafu.pig.urls;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * Given a valid URL, this UDF provides the following information about that URL:
 * Domain, Host, Protocol, Path, Port, Query Params and their values
 */
public class URLInfo extends EvalFunc<Tuple> {

    private static final int TUPLE_ELEMENTS = 6;
    private static Pattern pDomain = Pattern.compile(".*?([^.]+\\.[^.]+)");

    private static String getDomain(URL url) {
        Matcher m = pDomain.matcher(url.getHost());
        if (m.matches())
            return m.group(1);
        else
            return null;
    }

    private static String getHost(URL url) {
        return url.getHost();
    }

    private static String getProtocol(URL url) {
        return url.getProtocol();
    }

    private static String getPath(URL url) {
        return url.getPath();
    }

    private static Integer getPort(URL url) {
        int port = url.getPort();
        if (port == -1) return null;
        return port;
    }

    private static Map<String, String> getQueryParams(URL url) {
        String queryString = url.getQuery();
        if (queryString == null)
            return null;
        String[] qFields = queryString.split("&");
        String[] kv;
        Map<String, String> queryParams = new HashMap<String, String>();
        for (int i = 0; i < qFields.length; i++) {
            kv = qFields[i].split("=");
            // consider only valid query params AND skip duplicate keys
            if (kv.length == 2 && !(queryParams.containsKey(kv[0]))) {
                    queryParams.put(kv[0], kv[1]);
            }
        }
        return queryParams;
    }

    /**
     * Apache Pig UDF that provides information about URLs
     * @param tuple containing URL string
     * @return tuple containing domain name, host name, protocol, path,
     * port and query parameters (in that order)
     *
     */
    @Override
    public Tuple exec(Tuple tuple) throws IOException {
        URL url;
        final Tuple output = TupleFactory.getInstance()
                .newTuple(TUPLE_ELEMENTS);
        for (int i = 0; i < TUPLE_ELEMENTS; i++)
            output.set(i, null);
        if (tuple == null)
            return output;
        String input = tuple.get(0) == null ? "" : tuple.get(0).toString()
                .trim();
        try {
            url = new URL(input);
        } catch (MalformedURLException e) {
            return null;
        }

        output.set(0, getDomain(url));
        output.set(1, getHost(url));
        output.set(2, getProtocol(url));
        output.set(3, getPath(url));
        output.set(4, getPort(url));
        output.set(5, getQueryParams(url));
        return output;
    }

    @Override
    public Schema outputSchema(Schema input) {
        try {
            Schema tupleSchema = new Schema();
            tupleSchema.add(new Schema.FieldSchema(
                    "domain", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema(
                    "host", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema(
                    "protocol", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema(
                    "path", DataType.CHARARRAY));
            tupleSchema.add(new Schema.FieldSchema(
                    "port", DataType.INTEGER));
            tupleSchema.add(new Schema.FieldSchema(
                    "queryParams", DataType.MAP));

            return new Schema(new Schema.FieldSchema(
                    getSchemaName(this.getClass().getName().toLowerCase(),
                            input), tupleSchema, DataType.TUPLE));
        } catch (Exception e) {
            System.out.println("exception in URLInfo outputSchema: '" + e
                    + "'; returning null schema.\n");
            return null;
        }
    }

}
