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
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.cdc.WalRecordsConsumer;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.SpringApplicationContextResource;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.ApplicationContext;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.cdc.CdcLoader.loadCdc;
import static org.apache.ignite.internal.cdc.CdcMain.ERR_MSG;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class CdcConfigurationTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testLoadConfig() throws Exception {
        assertThrows(
            null,
            () -> loadCdc("modules/spring/src/test/config/cdc/double-ignite-config.xml"),
            IgniteCheckedException.class,
            "Exact 1 IgniteConfiguration should be defined. Found 2"
        );

        assertThrows(
            null,
            () -> loadCdc("modules/spring/src/test/config/cdc/double-cdc-config.xml"),
            IgniteCheckedException.class,
            "Exact 1 CaptureDataChangeConfiguration configuration should be defined. Found 2"
        );

        CdcMain cdc =
            loadCdc("modules/spring/src/test/config/cdc/cdc-config-without-persistence.xml");

        assertNotNull(cdc);

        assertThrows(null, cdc::run, IgniteException.class, ERR_MSG);
    }

    /** */
    @Test
    public void testInjectResources() throws Exception {
        CdcMain cdc =
            loadCdc("modules/spring/src/test/config/cdc/correct-cdc-config.xml");

        try (IgniteEx ign = startGrid((IgniteConfiguration)getFieldValue(cdc, "igniteCfg"))) {
            TestCdcConsumer cnsmr =
                (TestCdcConsumer)((WalRecordsConsumer<?, ?>)getFieldValue(cdc, "consumer")).consumer();

            assertNotNull(cnsmr);

            CountDownLatch startLatch = cnsmr.startLatch;

            IgniteInternalFuture<?> fut = runAsync(cdc::run);

            startLatch.await(getTestTimeout(), MILLISECONDS);

            assertEquals("someString", cnsmr.springString);
            assertEquals("someString2", cnsmr.springString2);
            assertNotNull(cnsmr.log);
            assertNotNull(cnsmr.ctx);

            fut.cancel();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    public static class TestCdcConsumer implements CdcConsumer {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @SpringResource(resourceName = "springString")
        private String springString;

        /** */
        private String springString2;

        /** */
        @SpringApplicationContextResource
        private ApplicationContext ctx;

        /** */
        public CountDownLatch startLatch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void start(MetricRegistry mreg) {
            springString2 = ctx.getBean("springString2", String.class);

            startLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override public boolean onEvents(Iterator<CdcEvent> events) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public void onTypes(Iterator<BinaryType> types) {
            // No-Op.
        }

        /** {@inheritDoc} */
        @Override public void onMappings(Iterator<TypeMapping> mappings) {
            // No-Op.
        }

        /** {@inheritDoc} */
        @Override public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents) {
            // No-Op.
        }

        /** {@inheritDoc} */
        @Override public void onCacheDestroy(Iterator<Integer> caches) {
            // No-Op.
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            // No-Op.
        }
    }
}
