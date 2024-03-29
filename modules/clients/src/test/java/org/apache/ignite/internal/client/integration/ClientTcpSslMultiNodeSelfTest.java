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

package org.apache.ignite.internal.client.integration;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests binary tcp protocol with SSL on multiple nodes.
 */
public class ClientTcpSslMultiNodeSelfTest extends ClientAbstractMultiNodeSelfTest {
    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return HOST + ":" + REST_TCP_PORT_BASE;
    }

    /** {@inheritDoc} */
    @Override protected Factory<SSLContext> sslContextFactory() {
        return GridTestUtils.sslFactory();
    }
}
