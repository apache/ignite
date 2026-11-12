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

package org.apache.ignite.ssl;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests that a context wrapper preserves engine addresses and applies its SSL parameters. */
public class SSLContextWrapperTest {
    /** Both engine overloads apply the parameters, and the addressed overload retains the peer host and port. */
    @Test
    public void testEnginePeerAddressAndParameters() throws Exception {
        SSLContext delegate = SSLContext.getInstance("TLS");

        delegate.init(null, null, null);

        String[] protocols = {"TLSv1.2"};
        String[] ciphers = {"TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"};
        String host = "peer.example";
        int port = 10800;
        SSLContext wrapper = new SSLContextWrapper(delegate, new SSLParameters(ciphers, protocols));
        SSLEngine addressed = wrapper.createSSLEngine(host, port);

        assertEquals(host, addressed.getPeerHost());
        assertEquals(port, addressed.getPeerPort());
        assertArrayEquals(protocols, addressed.getEnabledProtocols());
        assertArrayEquals(ciphers, addressed.getEnabledCipherSuites());

        SSLEngine unaddressed = wrapper.createSSLEngine();

        assertNull(unaddressed.getPeerHost());
        assertEquals(-1, unaddressed.getPeerPort());
        assertArrayEquals(protocols, unaddressed.getEnabledProtocols());
        assertArrayEquals(ciphers, unaddressed.getEnabledCipherSuites());
    }
}
