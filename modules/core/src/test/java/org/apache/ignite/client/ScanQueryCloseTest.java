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

package org.apache.ignite.client;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.QueryMXBeanImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.mxbean.QueryMXBean;
import org.apache.ignite.spi.systemview.view.ScanQueryView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsAnyCause;

/**
 * Tests scan query close.
 */
public class ScanQueryCloseTest extends GridCommonAbstractTest {
    /** Test logger. */
    private ListeningTestLogger testLog = new ListeningTestLogger(log);

    /** Query filter latch. */
    private static CountDownLatch filterLatch = new CountDownLatch(1);

    /** @throws Exception If failed. */
    @Test
    public void testScanQueryClose() throws Exception {
        try (Ignite srv1 = Ignition.start(Config.getServerConfiguration().setGridLogger(testLog));
             Ignite srv2 = Ignition.start(Config.getServerConfiguration().setGridLogger(testLog));

             IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))
        ) {
            ClientCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

            for (int i = 0; i < 10; i++)
                cache.put(i, i);

            ScanQuery<Object, Object> qry = new ScanQuery<>().setFilter((o, o2) -> {
                try {
                    filterLatch.countDown();

                    U.sleep(1000);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }

                return true;
            });

            LogListener lsnr = LogListener.matches("Iterator has been closed.").build();

            testLog.registerListener(lsnr);

            IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
                cache.query(qry).getAll();
            });

            filterLatch.await(getTestTimeout(), TimeUnit.SECONDS);

            killQueries();

            assertThrowsAnyCause(log, fut::get, IgniteCheckedException.class, null);

            assertTrue(lsnr.check());
        }
    }

    /** Kill all queries. */
    private static void killQueries() {
        List<Ignite> srvs = G.allGrids();

        assertFalse(srvs.isEmpty());

        QueryMXBean bean = getMxBean(srvs.get(0).name(), "Query",
            QueryMXBeanImpl.class.getSimpleName(), QueryMXBean.class);

        for (IgniteEx ignite : F.transform(G.allGrids(), ignite -> (IgniteEx)ignite)) {
            SystemView<ScanQueryView> qrySysView = ignite.context().systemView().view(SCAN_QRY_SYS_VIEW);

            qrySysView.iterator().forEachRemaining(view ->
                bean.cancelScan(view.originNodeId().toString(), view.cacheName(), view.queryId()));
        }
    }
}
