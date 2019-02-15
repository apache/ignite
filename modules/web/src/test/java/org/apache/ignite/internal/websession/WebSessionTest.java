/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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