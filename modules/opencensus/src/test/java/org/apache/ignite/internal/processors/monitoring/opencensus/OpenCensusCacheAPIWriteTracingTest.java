/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import com.google.common.collect.ImmutableMap;
import io.opencensus.trace.SpanId;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.tracing.Scope;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT;
import static org.apache.ignite.internal.processors.monitoring.opencensus.AbstractTracingTest.IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_DHT_UPDATE_FUTURE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_NEAR_UPDATE_FUTURE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_PUT;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_PUT_ALL;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_PUT_ALL_ASYNC;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_PUT_ASYNC;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_REMOVE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_REMOVE_ALL;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_REMOVE_ALL_ASYNC;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_REMOVE_ASYNC;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_UPDATE_MAP;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/**
 * Tests for checing atomic cache write api tracing:
 *
 * <ul>
 *     <li>put</li>
 *     <li>putAll</li>
 *     <li>putAsync</li>
 *     <li>putAllAsync</li>
 *     <li>remove</li>
 *     <li>removeAll</li>
 *     <li>removeAsync</li>
 *     <li>removeAllAsync</li>
 * </ul>
 */
@WithSystemProperty(key = IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT, value = IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL)
public class OpenCensusCacheAPIWriteTracingTest extends AbstractTracingTest {

    /** Client node. */
    private IgniteEx client;

    /** Cache name*/
    public static final String ATOMIC_CACHE = "AtomicCache";

    /** {@inheritDoc} */
    @Override protected TracingSpi getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /** {@inheritDoc} */
    @Override public void before() throws Exception {
        super.before();

        client = startClientGrid(GRID_CNT);

        client.getOrCreateCache(new CacheConfiguration<>(ATOMIC_CACHE).setBackups(2));

        awaitPartitionMapExchange();

        grid(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(Scope.CACHE_API_WRITE).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());
    }

    /**
     * <ol>
     *     <li>Run cache.put on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.put
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicPutTracing() throws Exception {
        client.cache(ATOMIC_CACHE).put("AnotherOne",1);

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_PUT,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "AnotherOne")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            1,
            null);

        List<SpanId> dhtUpdateFutSpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapSpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.putAll() on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.put.all
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>keys.count</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicPutAllTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAll(
            new HashMap<String, Integer>() {{
                put("One", 1);
                put("Two", 2);
                put("Three", 3);
            }});

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_PUT_ALL,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("keys.count", "3")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        List<SpanId> reqNearReqSpanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            2,
            null);

        // Futue 1.
        List<SpanId> dhtUpdateFutReq1SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL)
        );

        List<SpanId> dhtUpdateMapReq1SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq1SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq1SpanIds.get(0),
            1,
            null);

        // Future 2.
        List<SpanId> dhtUpdateFutReq2SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(1),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapReq2SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq2SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq2SpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.putAsync on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.put.async
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicPutAsyncTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAsync("One",1);

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_PUT_ASYNC,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "One")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            1,
            null);

        List<SpanId> dhtUpdateFutSpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapSpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.putAllAsync() on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.put.all.async
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>keys.count</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicPutAllAsyncTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAllAsync(
            new HashMap<String, Integer>() {{
                put("One", 1);
                put("Two", 2);
                put("Three", 3);
            }});

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_PUT_ALL_ASYNC,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("keys.count", "3")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        List<SpanId> reqNearReqSpanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            2,
            null);

        // Future 1.
        List<SpanId> dhtUpdateFutReq1SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapReq1SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq1SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq1SpanIds.get(0),
            1,
            null);

        // Future 2.
        List<SpanId> dhtUpdateFutReq2SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(1),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapReq2SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq2SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq2SpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.remove on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.remove
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicRemoveTracing() throws Exception {
        client.cache(ATOMIC_CACHE).put("One",1);

        client.cache(ATOMIC_CACHE).remove("One");

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_REMOVE,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "One")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            1,
            null);

        List<SpanId> dhtUpdateFutSpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapSpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.removeAll() on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.remove.all
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>keys.count</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicRemoveAllTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAll(
            new HashMap<String, Integer>() {{
                put("One", 1);
                put("Two", 2);
                put("Three", 3);
            }});

        client.cache(ATOMIC_CACHE).removeAll(
            new HashSet<String>() {{
                add("One");
                add("Two");
                add("Three");
            }});

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_REMOVE_ALL,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("keys.count", "3")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        List<SpanId> reqNearReqSpanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            2,
            null);

        // Future 1.
        List<SpanId> dhtUpdateFutReq1SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapReq1SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq1SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq1SpanIds.get(0),
            1,
            null);

        // Futue 2.
        List<SpanId> dhtUpdateFutReq2SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(1),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapReq2SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq2SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq2SpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.removeAsync on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.remove.async
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicRemoveAsyncTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAsync("One",1);

        client.cache(ATOMIC_CACHE).removeAsync("One");

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_REMOVE_ASYNC,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "One")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            1,
            null);

        List<SpanId> dhtUpdateFutSpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapSpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapSpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapSpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.removeAllAsync() on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.remove.all.async
     *      cache.api.near.update.future
     *          cache.api.near.update.map
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *              cache.api.near.process.atomic.update.request
     *                  cache.api.dht.update.future
     *                      cache.api.dht.update.map
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.dht.process.atomic.update.request
     *                          cache.api.near.process.atomic.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *                      cache.api.dht.process.atomic.deferred.update.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>keys.count</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicRemoveAllAsyncTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAllAsync(
            new HashMap<String, Integer>() {{
                put("One", 1);
                put("Two", 2);
                put("Three", 3);
            }});

        client.cache(ATOMIC_CACHE).removeAllAsync(
            new HashSet<String>() {{
                add("One");
                add("Two");
                add("Three");
            }});

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_REMOVE_ALL_ASYNC,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("keys.count", "3")
                .build()
        );

        spanIds = checkSpan(
            CACHE_API_NEAR_UPDATE_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            spanIds.get(0),
            1,
            null);

        List<SpanId> reqNearReqSpanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_REQUEST,
            spanIds.get(0),
            2,
            null);

        // Future 1.
        List<SpanId> dhtUpdateFutReq1SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(0),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapReq1SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq1SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq1SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq1SpanIds.get(0),
            1,
            null);

        // Futute 2.
        List<SpanId> dhtUpdateFutReq2SpanIds = checkSpanWithWaitForCondition(
            CACHE_API_DHT_UPDATE_FUTURE,
            reqNearReqSpanIds.get(1),
            1,
            null,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL) * 1000,
            Long.parseLong(IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT_VAL));

        List<SpanId> dhtUpdateMapReq2SpanIds = checkSpan(
            CACHE_API_UPDATE_MAP,
            dhtUpdateFutReq2SpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_DEFERRED_UPDATE_RESPONSE,
            dhtUpdateFutReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_DHT_PROCESS_ATOMIC_UPDATE_REQUEST,
            dhtUpdateMapReq2SpanIds.get(0),
            2,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_UPDATE_RESPONSE,
            dhtUpdateMapReq2SpanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.remove with valid value on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace contains cache.api.remove with following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicRemoveWithValidValTracing() throws Exception {
        client.cache(ATOMIC_CACHE).put("One",1);

        client.cache(ATOMIC_CACHE).remove("One", 1);

        handler().flush();

        checkSpan(
            CACHE_API_REMOVE,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "One")
                .build()
        );
    }

    /**
     * <ol>
     *     <li>Run cache.removeAsync with valid value on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace contains cache.api.remoce.async with following tags
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicRemoveAsyncWithValidValueTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAsync("One",1);

        client.cache(ATOMIC_CACHE).removeAsync("One", 1);

        handler().flush();

        checkSpan(
            CACHE_API_REMOVE_ASYNC,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "One")
                .build()
        );
    }

    /**
     * Check that in case of null key tracing won't fail.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testVerifyThatNullKeyDoNotFailTracing() throws Exception {
        GridTestUtils.assertThrows(
            log,
            () -> client.cache(ATOMIC_CACHE).put(null,1),
            NullPointerException.class,
            "Ouch! Argument cannot be null: key");

        handler().flush();

        checkSpan(
            CACHE_API_PUT,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "null")
                .build()
        );

        GridTestUtils.assertThrows(
            log,
            () -> client.cache(ATOMIC_CACHE).remove(null),
            NullPointerException.class,
            "Ouch! Argument cannot be null: key");

        handler().flush();

        checkSpan(
            CACHE_API_REMOVE,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put("node.id", client.localNode().id().toString())
                .put("node.consistent.id", client.localNode().consistentId().toString())
                .put("node.name", client.name())
                .put("cache", ATOMIC_CACHE)
                .put("key", "null")
                .build()
        );
    }
}
