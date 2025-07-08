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
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.startup.cmdline.CdcCommandLineStartup;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class CdcCommandLineStartupTest extends GridCommonAbstractTest {
    /** */
    private static final String IGNITE_INSTANCE_NAME = "ignite-name";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);
        cfg.setPeerClassLoadingEnabled(false);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setCdcEnabled(true).setPersistenceEnabled(true)));

        return cfg;
    }

    /** */
    @Test
    public void testCdcCommandLineStartup() throws Exception {
        try (IgniteEx ign = startGrid(IGNITE_INSTANCE_NAME)) {
            ign.cluster().state(ACTIVE);

            IgniteInternalFuture<?> fut = startCdcCommandLineConsumer();

            assertTrue(waitForCondition(() -> ign.cluster().nodes().size() == 2, getTestTimeout()));

            fut.cancel();

            assertTrue(waitForCondition(() -> ign.cluster().nodes().size() == 1, getTestTimeout()));
        }
    }

    /** */
    private IgniteInternalFuture<?> startCdcCommandLineConsumer() {
        String cfgPath = "modules/spring/src/test/config/cdc/cdc-command-line-consumer.xml";

        return runAsync(() -> CdcCommandLineStartup.main(new String[] {cfgPath}), "cmdline-consumer");
    }

    /** */
    public static class CdcCommandLineConsumer implements CdcConsumer {
        /** Destination cluster client configuration. */
        private IgniteConfiguration destIgniteCfg;

        /** Destination Ignite cluster client */
        private IgniteEx dest;


        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            A.notNull(destIgniteCfg, "Destination Ignite configuration.");

            dest = (IgniteEx)Ignition.start(destIgniteCfg);
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
            dest.close();
        }

        /**
         * Sets Ignite client node configuration that will connect to destination cluster.
         * @param destIgniteCfg Ignite client node configuration that will connect to destination cluster.
         * @return {@code this} for chaining.
         */
        public CdcCommandLineConsumer setDestinationIgniteConfiguration(IgniteConfiguration destIgniteCfg) {
            this.destIgniteCfg = destIgniteCfg;

            return this;
        }
    }
}
