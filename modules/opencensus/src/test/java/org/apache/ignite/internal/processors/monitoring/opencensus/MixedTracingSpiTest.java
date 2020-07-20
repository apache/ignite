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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.spi.tracing.NoopTracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for mixed tracing spi instances.
 */
public class MixedTracingSpiTest extends GridCommonAbstractTest {

    /** */
    private ListeningTestLogger testLog = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Prepares log listeners list.
     *
     * @return List of prepared log listeners.
     */
    private List<LogListener> prepareLogListeners() {
        testLog.clearListeners();

        List<LogListener> listeners = new ArrayList<>();

        listeners.add(LogListener.matches(
            ">>> Remote SPI with the same name is not configured: OpenCensusTracingSpi").build());

        listeners.add(LogListener.matches(
            ">>> Remote SPI with the same name is not configured: NoopTracingSpi").build());

        listeners.forEach(testLog::registerListener);

        return listeners;
    }

    /**
     * Start two nodes: one with noop tracing spi and another one with open census tracing spi.
     * <p/>
     * Cause both tracing spi instances are annotated with {@code @IgniteSpiConsistencyChecked(optional = true)}
     * 'Remote SPI with the same name is not configured: <TracingSpiInstance></>' warning message is expected on
     * node start.
     * <p/>
     * Besides that 'Failed to create span from serialized value' is expected, cause it's not possible
     * to deserialize {@code NoopSpan.INSTANCE} from {@link NoopTracingSpi} on the node
     * with {@link OpenCensusTracingSpi}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodesWithDifferentTracingSpiPrintsWarningOnConsistencyCheck() throws Exception {
        List<LogListener> listeners = prepareLogListeners();

        startGrid(getConfiguration(getTestIgniteInstanceName(0) +
            "node-with-noop-tracing").setTracingSpi(new NoopTracingSpi()).setGridLogger(testLog));

        startGrid(getConfiguration(getTestIgniteInstanceName(1) +
            "node-with-open-census-tracing").setTracingSpi(new OpenCensusTracingSpi()).setGridLogger(testLog));

        listeners.forEach(lsnr -> assertTrue(lsnr.check()));
    }

    /**
     * Start two nodes, both with noop tracing spi instances. No warning messages are expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodesWithNoopTracingSpiPrintsNothingOnConsistencyCheck() throws Exception {
        List<LogListener> listeners = prepareLogListeners();

        startGrid(getConfiguration(getTestIgniteInstanceName(0) +
            "node-with-noop-tracing").setTracingSpi(new NoopTracingSpi()).setGridLogger(testLog));

        startGrid(getConfiguration(getTestIgniteInstanceName(1) +
            "node-with-noop-tracing").setTracingSpi(new NoopTracingSpi()).setGridLogger(testLog));

        listeners.forEach(lsnr -> assertFalse(lsnr.check()));
    }

    /**
     * Start two nodes, both with open census tracing spi instances. No warning messages are expected.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodesWithOpenCensusTracingSpiPrintsNothingOnConsistencyCheck() throws Exception {
        List<LogListener> listeners = prepareLogListeners();

        startGrid(getConfiguration(getTestIgniteInstanceName(0) +
            "node-with-open-census-tracing").setTracingSpi(new OpenCensusTracingSpi()).setGridLogger(testLog));

        startGrid(getConfiguration(getTestIgniteInstanceName(1) +
            "node-with-open-census-tracing").setTracingSpi(new OpenCensusTracingSpi()).setGridLogger(testLog));

        listeners.forEach(lsnr -> assertFalse(lsnr.check()));
    }
}
