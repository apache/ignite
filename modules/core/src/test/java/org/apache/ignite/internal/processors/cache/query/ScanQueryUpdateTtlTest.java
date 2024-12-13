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

package org.apache.ignite.internal.processors.cache.query;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.expiry.Duration;
import javax.cache.expiry.TouchedExpiryPolicy;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTtlUpdateRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.query.ScanQueryIterator.EXPIRE_ENTRIES_FLUSH_CNT;

/** */
public class ScanQueryUpdateTtlTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS = EXPIRE_ENTRIES_FLUSH_CNT * 3;

    /** */
    AtomicBoolean failed = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setBackups(1)
                .setExpiryPolicyFactory(TouchedExpiryPolicy.factoryOf(new Duration(TimeUnit.MINUTES, 60))));
        }

        cfg.setCommunicationSpi(new CheckingCommunicationSpi(failed));

        return cfg;
    }

    /** */
    @Test
    public void testScanQueryIteratorExpireEntriesFlush() throws Exception {
        try (IgniteEx srv = startGrids(2)) {
            srv.cluster().state(ClusterState.ACTIVE);

            try (IgniteEx cln = startClientGrid(2)) {
                for (int i = 0; i < KEYS; i++)
                    cln.cache(DEFAULT_CACHE_NAME).put(i, i);

                cln.cache(DEFAULT_CACHE_NAME).query(new ScanQuery<>()).forEach((v) -> {});
            }

            grid(0).context().pools().getStripedExecutorService().awaitComplete();
            grid(1).context().pools().getStripedExecutorService().awaitComplete();

            assertFalse("GridCacheTtlUpdateRequest was sent with more then maximum allowed keys [max=" +
                (EXPIRE_ENTRIES_FLUSH_CNT + 1) + "]", failed.get());
        }
    }

    /** */
    private static class CheckingCommunicationSpi extends TestRecordingCommunicationSpi {
        /** */
        private final AtomicBoolean failed;

        /** */
        CheckingCommunicationSpi(AtomicBoolean failed) {
            this.failed = failed;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
                    throws IgniteSpiException {
            if ((msg instanceof GridIoMessage) && (((GridIoMessage)msg).message() instanceof GridCacheTtlUpdateRequest)) {
                GridCacheTtlUpdateRequest ttlUpdReq = (GridCacheTtlUpdateRequest)((GridIoMessage)msg).message();

                System.out.println("SIZE " + ttlUpdReq.keys().size());

                if (ttlUpdReq.keys().size() > EXPIRE_ENTRIES_FLUSH_CNT + 1)
                    failed.set(true);
            }
            super.sendMessage(node, msg, ackC);
        }
    }
}
