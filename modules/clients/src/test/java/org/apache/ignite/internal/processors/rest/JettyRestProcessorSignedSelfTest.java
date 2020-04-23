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

package org.apache.ignite.internal.processors.rest;

import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 *
 */
public class JettyRestProcessorSignedSelfTest extends JettyRestProcessorAbstractSelfTest {
    /** */
    protected static final String REST_SECRET_KEY = "secret-key";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        assert cfg.getConnectorConfiguration() != null;

        cfg.getConnectorConfiguration().setSecretKey(REST_SECRET_KEY);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int restPort() {
        return 8092;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUnauthorized() throws Exception {
        String addr = "http://" + LOC_HOST + ":" + restPort() + "/ignite?cacheName=default&cmd=top";

        URL url = new URL(addr);

        URLConnection conn = url.openConnection();

        // Request has not been signed.
        conn.connect();

        assert ((HttpURLConnection)conn).getResponseCode() == 401;

        // Request with authentication info.
        addr = "http://" + LOC_HOST + ":" + restPort() + "/ignite?cacheName=default&cmd=top";

        url = new URL(addr);

        conn = url.openConnection();

        conn.setRequestProperty("X-Signature", signature());

        conn.connect();

        assertEquals(200, ((HttpURLConnection)conn).getResponseCode());
    }

    /** {@inheritDoc} */
    @Override protected String signature() throws Exception {
        long ts = U.currentTimeMillis();

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");

            String s = ts + ":" + REST_SECRET_KEY;

            md.update(s.getBytes());

            String hash = Base64.getEncoder().encodeToString(md.digest());

            return ts + ":" + hash;
        }
        catch (NoSuchAlgorithmException e) {
            throw new Exception("Failed to create authentication signature.", e);
        }
    }
}
