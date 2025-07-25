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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.startup.cmdline.CdcCommandLineStartup;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcCommandLineStartupTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setCdcEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testCdcCommandLineStartup() throws Exception {
        try (IgniteEx ign = startGrid(0)) {
            IgniteInternalFuture<?> fut = runAsync(() ->
                CdcCommandLineStartup.main(new String[] {"modules/spring/src/test/config/cdc/cdc-command-line-consumer.xml"}),
                "cmdline-consumer"
            );

            assertTrue(waitForCondition(CdcCommandLineConsumer::started, getTestTimeout()));

            fut.cancel();

            assertTrue(fut.isDone());

            assertTrue(CdcCommandLineConsumer.interrupted());
        }
    }

    /** */
    public static class CdcCommandLineConsumer implements CdcConsumer {
        /** */
        private static final AtomicBoolean started = new AtomicBoolean(false);

        /** */
        private static final AtomicBoolean interrupted = new AtomicBoolean(false);

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            try {
                started.compareAndSet(false, true);

                while (true)
                    Thread.sleep(100);
            }
            catch (InterruptedException e) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException ex) {
                    // No-op.
                }

                interrupted.compareAndSet(false, true);

                Thread.currentThread().interrupt();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<CdcEvent> events) {
            events.forEachRemaining(evt -> {
                // No-op.
            });

            return true;
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            types.forEachRemaining(type -> {
                // No-op.
            });
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            mappings.forEachRemaining(mapping -> {
                // No-op.
            });
        }

        /** {@inheritDoc} */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
            cacheEvents.forEachRemaining(cacheEvt -> {
                // No-op.
            });
        }

        /** {@inheritDoc} */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            caches.forEachRemaining(cache -> {
                // No-op.
            });
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            started.compareAndSet(true, false);
        }

        /** */
        public static boolean started() {
            return started.get();
        }

        /** */
        public static boolean interrupted() {
            return interrupted.get();
        }
    }
}
