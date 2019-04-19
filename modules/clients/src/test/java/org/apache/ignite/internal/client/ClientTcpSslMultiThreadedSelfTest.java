/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.client;

import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Runs multithreaded tests for client over tcp binary protocol with ssl enabled.
 */
public class ClientTcpSslMultiThreadedSelfTest extends ClientAbstractMultiThreadedSelfTest {
    /** {@inheritDoc} */
    @Override protected GridClientProtocol protocol() {
        return GridClientProtocol.TCP;
    }

    /** {@inheritDoc} */
    @Override protected String serverAddress() {
        return HOST + ":" + REST_TCP_PORT_BASE;
    }

    /** {@inheritDoc} */
    @Override protected boolean useSsl() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected GridSslContextFactory sslContextFactory() {
        return GridTestUtils.sslContextFactory();
    }
}