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

import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_CUSTOM_EVENT;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_JOIN_REQUEST;
import static org.apache.ignite.internal.processors.tracing.SpanType.DISCOVERY_NODE_LEFT;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

/**
 * Test to check tracing for operations that were started on client node.
 */
public class OpenCensusDiscoveryTracingTest extends AbstractTracingTest {
    /** */
    private static final String CACHE_NAME = "CacheName";

    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /** {@inheritDoc} */
    @Override public void before() throws Exception {
        super.before();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopGrid(GRID_CNT);
    }

    /**
     * Checks that there's DISCOVERY_NODE_JOIN_REQUEST on coordinator with corresponding 'event.node.id'
     *  that matches started client node.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testClientNodeStartTracing() throws Exception {
        performClientBasedOperation(
            null,
            () -> {
                try {
                    startClientGrid(GRID_CNT);
                }
                catch (Exception e) {
                    fail("Unable to start client grid.");
                }
            }
        );

        assertTrue(
            checkSpanExistences(
                DISCOVERY_NODE_JOIN_REQUEST,
                ImmutableMap.<String, String>builder()
                    .put("node.id", grid(0).localNode().id().toString())
                    .put("node.consistent.id", grid(0).localNode().consistentId().toString())
                    .put("node.name", grid(0).name())
                    .put("event.node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("event.node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .build()
            )
        );
    }

    /**
     * Checks that creating cache produces DISCOVERY_CUSTOM_EVENT span
     *  with corresponding 'DynamicCacheChangeBatch' message class on client grid.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testClientNodeCacheCreateTracing() throws Exception {
        performClientBasedOperation(
            () -> {
                try {
                    startClientGrid(GRID_CNT);
                }
                catch (Exception e) {
                    fail("Unable to start client grid.");
                }
            },
            () -> {
                try {
                    grid(GRID_CNT).getOrCreateCache(CACHE_NAME);
                }
                catch (Exception e) {
                    fail("Unable to create cache.");
                }
            }
        );

        assertTrue(
            checkSpanExistences(
                DISCOVERY_CUSTOM_EVENT,
                ImmutableMap.<String, String>builder()
                    .put("node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .put("node.name", grid(GRID_CNT).name())
                    .put("event.node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("message.class", "DynamicCacheChangeBatch")
                    .put("event.node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .build()
            )
        );
    }

    /**
     * Checks that binary type registration produces two DISCOVERY_CUSTOM_EVENT spans
     *  with corresponding 'MappingProposedMessage' and 'MetadataUpdateProposedMessage' message classes on client grid.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testClientNodeBinaryTypeRegistrationTracing() throws Exception {
        performClientBasedOperation(
            () -> {
                try {
                    startClientGrid(GRID_CNT);

                    grid(GRID_CNT).getOrCreateCache(CACHE_NAME);
                }
                catch (Exception e) {
                    fail("Unable to either start client grid or create cache.");
                }
            },
            () -> {
                try {
                    grid(GRID_CNT).cache(CACHE_NAME).put(1, new Value(1));
                }
                catch (Exception e) {
                    fail("Unable to create cache.");
                }
            });

        assertTrue(
            checkSpanExistences(
                DISCOVERY_CUSTOM_EVENT,
                ImmutableMap.<String, String>builder()
                    .put("node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .put("node.name", grid(GRID_CNT).name())
                    .put("event.node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("message.class", "MappingProposedMessage")
                    .put("event.node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .build()
            )
        );

        assertTrue(
            checkSpanExistences(
                DISCOVERY_CUSTOM_EVENT,
                ImmutableMap.<String, String>builder()
                    .put("node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .put("node.name", grid(GRID_CNT).name())
                    .put("event.node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("message.class", "MetadataUpdateProposedMessage")
                    .put("event.node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .build()
            )
        );
    }

    /**
     * Checks that cache destruction produces DISCOVERY_CUSTOM_EVENT span
     *  with corresponding 'DynamicCacheChangeBatch' message class on client grid.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testClientNodeCacheDestructionTracing() throws Exception {
        performClientBasedOperation(
            () -> {
                try {
                    startClientGrid(GRID_CNT);

                    grid(GRID_CNT).getOrCreateCache(CACHE_NAME);
                }
                catch (Exception e) {
                    fail("Unable to either start client grid or create cache.");
                }
            },
            () -> {
                try {
                    grid(GRID_CNT).destroyCache(CACHE_NAME);
                }
                catch (Exception e) {
                    fail("Unable to create cache.");
                }
            });

        assertTrue(
            checkSpanExistences(
                DISCOVERY_CUSTOM_EVENT,
                ImmutableMap.<String, String>builder()
                    .put("node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .put("node.name", grid(GRID_CNT).name())
                    .put("event.node.id", grid(GRID_CNT).localNode().id().toString())
                    .put("message.class", "DynamicCacheChangeBatch")
                    .put("event.node.consistent.id", grid(GRID_CNT).localNode().consistentId().toString())
                    .build()
            )
        );
    }

    /**
     * Checks that cache destruction produces DISCOVERY_NODE_LEFT span
     *  with corresponding 'event.node.id' on coordinator.
     *
     * @throws Exception If Failed.
     */
    @Test
    public void testClientNodeStopTracing() throws Exception {
        Map<String, String> clientNodeMeta = new HashMap<>();

        performClientBasedOperation(
            () -> {
                try {
                    startClientGrid(GRID_CNT);

                    clientNodeMeta.put("event.node.id", grid(GRID_CNT).localNode().id().toString());
                }
                catch (Exception e) {
                    fail("Unable to either start client grid or create cache.");
                }
            },
            () -> {
                try {
                    stopGrid(GRID_CNT);
                }
                catch (Exception e) {
                    fail("Unable to create cache.");
                }
            });

        assertTrue(
            checkSpanExistences(
                DISCOVERY_NODE_LEFT,
                ImmutableMap.<String, String>builder()
                    .put("node.id", grid(0).localNode().id().toString())
                    .put("node.consistent.id", grid(0).localNode().consistentId().toString())
                    .put("node.name", grid(0).name())
                    .put("event.node.id", clientNodeMeta.get("event.node.id"))
                    .build()
            )
        );
    }

    /**
     * Perform preparation operations and operation to trigger span creation.
     *
     * @param preparationOperations Operations to run without tracing.
     * @param operationToCheck Operations to run with tracing.
     * @throws Exception If failed.
     */
    private void performClientBasedOperation(
        @Nullable Runnable preparationOperations,
        @NotNull Runnable operationToCheck
    ) throws Exception {
        if (preparationOperations != null) {
            try {
                preparationOperations.run();
            }
            catch (Exception e) {
                fail("Unable to perform preparation operations. " + e);
            }
        }

        // Enable discovery tracing.
        grid(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.DISCOVERY).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        try {
            operationToCheck.run();
        }
        catch (Exception e) {
            fail("Unable to perform operation that is supposed to be traced." + e);
        }

        handler().flush();

        // Disable discovery tracing in order not to mess with un-relevant spans.
        grid(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.DISCOVERY).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_NEVER).build());
    }

    /**
     * Test Value class to be registered within one of the tests.
     */
    public static class Value {
        /** */
        final int val;

        /**
         * Constructor.
         *
         * @param val Value to register.
         */
        public Value(int val) {
            this.val = val;
        }
    }
}
