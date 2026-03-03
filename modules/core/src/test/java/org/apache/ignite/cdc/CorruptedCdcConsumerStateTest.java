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

package org.apache.ignite.cdc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class CorruptedCdcConsumerStateTest extends AbstractCdcTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setWalForceArchiveTimeout(100)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setCdcEnabled(true)
            ));

        return cfg;
    }

    /** */
    @Test
    public void testCdcMainClearsCorruptedFiles() throws Exception {
        try (Ignite ign = startGrid(0)) {
            CountDownLatch sendCacheLatch = new CountDownLatch(1);

            CdcMain cdc = createCdc(new TestCdcConsumer(sendCacheLatch), ign.configuration());

            IgniteInternalFuture<?> cdcFut = runAsync(cdc);

            // Force writing consumer state to ./state directory.
            try {
                ign.getOrCreateCache(DEFAULT_CACHE_NAME).put(0, 0);

                U.await(sendCacheLatch);
            }
            finally {
                cdcFut.cancel();
            }

            // Corrupt data.
            NodeFileTree ft = GridTestUtils.getFieldValue(cdc, "ft");

            Path cdcCacheState = ft.cdcCachesState();

            byte[] corrupted = new byte[10];
            ThreadLocalRandom.current().nextBytes(corrupted);

            Files.write(cdcCacheState, corrupted);

            sendCacheLatch = new CountDownLatch(1);

            cdcFut = runAsync(createCdc(new TestCdcConsumer(sendCacheLatch), ign.configuration()));
            cdcFut.listen(sendCacheLatch::countDown);

            // Force writing consumer state to ./state directory.
            try {
                ign.getOrCreateCache(DEFAULT_CACHE_NAME).put(0, 0);

                U.await(sendCacheLatch);

                assertFalse(cdcFut.isDone());
                assertNull(cdcFut.error());
            }
            finally {
                cdcFut.cancel();
            }
        }
    }

    /** */
    private static final class TestCdcConsumer implements CdcConsumer {
        /** */
        private final CountDownLatch latch;

        /** */
        TestCdcConsumer(CountDownLatch latch) {
            this.latch = latch;
        }

        /** */
        @Override public void start(MetricRegistry mreg) {
            // No-op.
        }

        /** */
        @Override public boolean onEvents(Iterator<CdcEvent> evts) {
            return false;
        }

        /** */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(t -> {});
        }

        /** */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            mappings.forEachRemaining(t -> {});
        }

        /** */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvts) {
            cacheEvts.forEachRemaining(e -> {
                if (e.cacheId() == CU.cacheId(DEFAULT_CACHE_NAME))
                    latch.countDown();
            });
        }

        /** */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            caches.forEachRemaining(c -> {});
        }

        /** */
        @Override public void stop() {
            // No-op.
        }
    }
}
