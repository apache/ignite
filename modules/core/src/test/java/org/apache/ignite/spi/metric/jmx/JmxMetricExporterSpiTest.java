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

package org.apache.ignite.spi.metric.jmx;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricManager;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.IgniteTestResources;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class JmxMetricExporterSpiTest extends GridCommonAbstractTest {
    /**
     *
     */
    @Test
    public void testConcurrentRegistration() throws IgniteCheckedException {
        JmxMetricExporterSpi spi = new JmxMetricExporterSpi();

        new IgniteTestResources(new DummyMBeanServer()).inject(spi);

        TestMetricsManager testMgr = new TestMetricsManager();

        spi.setMetricRegistry(testMgr);

        spi.spiStart("testInstance");

        testMgr.runRegistersConcurrent();
        testMgr.runUnregisters();
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    private static class TestMetricsManager implements ReadOnlyMetricManager {
        /** */
        private final List<Consumer<ReadOnlyMetricRegistry>> creation = new ArrayList<>();

        /** */
        private final List<Consumer<ReadOnlyMetricRegistry>> rmv = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void addMetricRegistryCreationListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
            creation.add(lsnr);
        }

        /** {@inheritDoc} */
        @Override public void addMetricRegistryRemoveListener(Consumer<ReadOnlyMetricRegistry> lsnr) {
            rmv.add(lsnr);
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<ReadOnlyMetricRegistry> iterator() {
            return EmptyIterator.INSTANCE;
        }

        /**
         *
         */
        public void runRegistersConcurrent() {
            final AtomicInteger cntr = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(() -> {
                for (int i = 0; i < 20; i++) {
                    for (Consumer<ReadOnlyMetricRegistry> lsnr : creation)
                        lsnr.accept(new ReadOnlyMetricRegistryStub("stub-" + cntr.getAndIncrement()));
                }
            }, Runtime.getRuntime().availableProcessors() * 2, "runner-");

        }

        /**
         *
         */
        public void runUnregisters() {
            for (int i = 0; i < Runtime.getRuntime().availableProcessors() * 2 * 20; i++) {
                for (Consumer<ReadOnlyMetricRegistry> lsnr : creation)
                    lsnr.accept(new ReadOnlyMetricRegistryStub("stub-" + i));
            }
        }

        /**
         *
         */
        private static class ReadOnlyMetricRegistryStub implements ReadOnlyMetricRegistry {
            /** */
            private final String name;

            /**
             * @param name Stub name.
             */
            private ReadOnlyMetricRegistryStub(String name) {
                this.name = name;
            }

            /** {@inheritDoc} */
            @Override public String name() {
                return name;
            }

            /** {@inheritDoc} */
            @Override public <M extends Metric> @Nullable M findMetric(String name) {
                return null;
            }

            /** {@inheritDoc} */
            @NotNull @Override public Iterator<Metric> iterator() {
                return EmptyIterator.INSTANCE;
            }
        }
    }
}
