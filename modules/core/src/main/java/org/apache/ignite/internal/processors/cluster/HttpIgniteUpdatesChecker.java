/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cluster;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * This class is responsible for getting Ignite updates information via HTTP
 */
public class HttpIgniteUpdatesChecker {
    /** Url for request updates. */
    private final String url;

    /** Params. */
    private final String params;

    /** Charset for encoding requests/responses */
    private final String charset;

    /**
     * Creates new HTTP Ignite updates checker with following parameters
     * @param url URL for getting Ignite updates information
     * @param charset Charset for encoding
     */
    HttpIgniteUpdatesChecker(String url, String params, String charset) {
        this.url = url;
        this.params = params;
        this.charset = charset;
    }

    /**
     * Gets information about Ignite updates via HTTP
     * @return Information about Ignite updates separated by line endings
     * @throws IOException If HTTP request was failed
     */
    public String getUpdates(boolean first) throws IOException {
        String addr = first ? url + params : url;

        URLConnection conn = new URL(addr).openConnection();
        conn.setRequestProperty("Accept-Charset", charset);

        conn.setConnectTimeout(3000);
        conn.setReadTimeout(3000);

        try (InputStream in = conn.getInputStream()) {
            if (in == null)
                return null;

            BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset));

            StringBuilder res = new StringBuilder();

            for (String line; (line = reader.readLine()) != null; )
                res.append(line).append('\n');

            return res.toString();
        }
    }
}
