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

package org.apache.ignite.internal.websession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Web sessions caching test.
 */
public class WebSessionTest {
    /** */
    private static final AtomicReference<String> SES_ID = new AtomicReference<>();

    /**
     * @param args Arguments.
     * @throws Exception In case of error.
     */
    public static void main(String[] args) throws Exception {
        doRequest(8091, false);
        doRequest(8092, true);
        doRequest(8092, true);
        doRequest(8092, true);
    }

    /**
     * @param port Port.
     * @param addCookie Whether to add cookie to request.
     * @throws IOException In case of I/O error.
     */
    private static void doRequest(int port, boolean addCookie) throws IOException {
        URLConnection conn = new URL("http://localhost:" + port + "/ignitetest/test").openConnection();

        if (addCookie)
            conn.addRequestProperty("Cookie", "JSESSIONID=" + SES_ID.get());

        conn.connect();

        BufferedReader rdr = new BufferedReader(new InputStreamReader(conn.getInputStream()));

        if (!addCookie)
            SES_ID.set(rdr.readLine());
        else
            rdr.read();
    }
}