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

import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** Communication keeps one SSL context: new connections do not call the SSL context factory on either node. */
public class SslContextReuseTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** Reopening a communication connection does not call the SSL context factory on either node. */
    @Test
    public void testCommunicationReusesContextOnReconnect() throws Exception {
        Factory<SSLContext> sslFactory = GridTestUtils.sslFactory();
        AtomicInteger created = new AtomicInteger();
        Factory<SSLContext> counting = () -> {
            created.incrementAndGet();

            return sslFactory.create();
        };

        IgniteEx first = startGrid(0, cfg -> {
            cfg.setSslContextFactory(counting);
        });
        IgniteEx second = startGrid(1, cfg -> {
            cfg.setSslContextFactory(counting);
        });
        int createdAtStart = created.get();

        ConnectionClientPool pool = U.field(first.configuration().getCommunicationSpi(), "clientPool");

        pool.forceCloseConnection(second.localNode().id());
        first.compute(first.cluster().forNodeId(second.localNode().id())).run(() -> {});

        assertEquals(createdAtStart, created.get());
    }
}
