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
import org.junit.Test;

import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_DHT_GET_FUTURE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_DHT_SINGLE_GET_FUTURE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_GET;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_GET_ALL;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_GET_ALL_ASYNC;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_GET_ASYNC;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_GET_MAP;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_NEAR_PROCESS_ATOMIC_GET_REQUEST;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_NEAR_PROCESS_ATOMIC_GET_RESPONSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_NEAR_PROCESS_ATOMIC_SINGLE_GET_REQUEST;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_NEAR_PROCESS_ATOMIC_SINGLE_GET_RESPONSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_PARTITIONED_GET_FUTURE;
import static org.apache.ignite.internal.processors.tracing.SpanType.CACHE_API_PARTITIONED_SINGLE_GET_FUTURE;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;

/**
 * Tests for checing atomic cache read api tracing:
 *
 * <ul>
 *     <li>get</li>
 *     <li>getAll</li>
 *     <li>getAsync</li>
 *     <li>getAllAsync</li>
 * </ul>
 */
public class OpenCensusCacheAPIReadTracingTest extends AbstractTracingTest {

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
            new TracingConfigurationCoordinates.Builder(Scope.CACHE_API_READ).build(),
            new TracingConfigurationParameters.Builder().
                withSamplingRate(SAMPLING_RATE_ALWAYS).build());
    }

    /**
     * <ol>
     *     <li>Run cache.get on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.get
     *      cache.api.partitioned.single.get.future
     *          cache.api.partitioned.single.get.map
     *              cache.api.near.atomic.single.get.request
     *                  cache.api.dht.single.get.future
     *                      cache.api.dht.single.get.map
     *                  cache.api.near.atomic.single.get.response
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
    public void testCacheAtomicGetTracing() throws Exception {
        client.cache(ATOMIC_CACHE).put("One",1);

        client.cache(ATOMIC_CACHE).get("One");

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_GET,
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
            CACHE_API_PARTITIONED_SINGLE_GET_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_GET_MAP,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_SINGLE_GET_REQUEST,
            spanIds.get(0),
            1,
            null);

        List<SpanId> dhtGetFutSpanIds = checkSpan(
            CACHE_API_DHT_SINGLE_GET_FUTURE,
            spanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_GET_MAP,
            dhtGetFutSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_SINGLE_GET_RESPONSE,
            spanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.getAll on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.get
     *      cache.api.partitioned.get.future
     *          cache.api.partitioned.get.map
     *              cache.api.near.atomic.get.request
     *                  cache.api.dht.get.future
     *                      cache.api.dht.get.map
     *                  cache.api.near.atomic.get.response
     *              cache.api.near.atomic.get.request
     *                  cache.api.dht.get.future
     *                      cache.api.dht.get.map
     *                  cache.api.near.atomic.get.response
     *              cache.api.near.atomic.get.request
     *                  cache.api.dht.get.future
     *                      cache.api.dht.get.map
     *                  cache.api.near.atomic.get.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key.count</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicGetAllTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAll(
            new HashMap<String, Integer>() {{
                put("One", 1);
                put("Two", 2);
                put("Three", 3);
            }});

        client.cache(ATOMIC_CACHE).getAll(
            new HashSet<String>() {{
                add("One");
                add("Two");
                add("Three");
            }});

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_GET_ALL,
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
            CACHE_API_PARTITIONED_GET_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_GET_MAP,
            spanIds.get(0),
            1,
            null);

        List<SpanId> reqSpanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_GET_REQUEST,
            spanIds.get(0),
            2,
            null);

        // Futute 1.
        List<SpanId> dhtGetFutSpanIds1 = checkSpan(
            CACHE_API_DHT_GET_FUTURE,
            reqSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_GET_MAP,
            dhtGetFutSpanIds1.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_GET_RESPONSE,
            reqSpanIds.get(0),
            1,
            null);

        // Futue 2.
        List<SpanId> dhtGetFutSpanIds2 = checkSpan(
            CACHE_API_DHT_GET_FUTURE,
            reqSpanIds.get(1),
            1,
            null);

        checkSpan(
            CACHE_API_GET_MAP,
            dhtGetFutSpanIds2.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_GET_RESPONSE,
            reqSpanIds.get(1),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.getAsync on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.get.async
     *      cache.api.partitioned.single.get.future
     *          cache.api.partitioned.single.get.map
     *              cache.api.near.atomic.single.get.request
     *                  cache.api.dht.single.get.future
     *                      cache.api.dht.single.get.map
     *                  cache.api.near.atomic.single.get.response
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
    public void testCacheAtomicGetAsyncTracing() throws Exception {
        client.cache(ATOMIC_CACHE).put("One",1);

        client.cache(ATOMIC_CACHE).getAsync("One");

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_GET_ASYNC,
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
            CACHE_API_PARTITIONED_SINGLE_GET_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_GET_MAP,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_SINGLE_GET_REQUEST,
            spanIds.get(0),
            1,
            null);

        List<SpanId> dhtGetFutSpanIds = checkSpan(
            CACHE_API_DHT_SINGLE_GET_FUTURE,
            spanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_GET_MAP,
            dhtGetFutSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_SINGLE_GET_RESPONSE,
            spanIds.get(0),
            1,
            null);
    }

    /**
     * <ol>
     *     <li>Run cache.getAllAsync on atomic cache with two backups.</li>
     * </ol>
     *
     * Check that got trace is equals to:
     *  cache.api.get.all.async
     *      cache.api.partitioned.get.future
     *          cache.api.partitioned.get.map
     *              cache.api.near.atomic.get.request
     *                  cache.api.dht.get.future
     *                      cache.api.dht.get.map
     *                  cache.api.near.atomic.get.response
     *              cache.api.near.atomic.get.request
     *                  cache.api.dht.get.future
     *                      cache.api.dht.get.map
     *                  cache.api.near.atomic.get.response
     *              cache.api.near.atomic.get.request
     *                  cache.api.dht.get.future
     *                      cache.api.dht.get.map
     *                  cache.api.near.atomic.get.response
     *
     *   <p>
     *   Also check that root cache.api.put span contains following tags:
     *   <ol>
     *       <li>node.id</li>
     *       <li>node.consistent.id</li>
     *       <li>node.name</li>
     *       <li>cache</li>
     *       <li>key.count</li>
     *   </ol>
     *
     */
    @Test
    public void testCacheAtomicGetAsyncAllTracing() throws Exception {
        client.cache(ATOMIC_CACHE).putAll(
            new HashMap<String, Integer>() {{
                put("One", 1);
                put("Two", 2);
                put("Three", 3);
            }});

        client.cache(ATOMIC_CACHE).getAllAsync(
            new HashSet<String>() {{
                add("One");
                add("Two");
                add("Three");
            }});

        handler().flush();

        List<SpanId> spanIds = checkSpan(
            CACHE_API_GET_ALL_ASYNC,
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
            CACHE_API_PARTITIONED_GET_FUTURE,
            spanIds.get(0),
            1,
            null);

        spanIds = checkSpan(
            CACHE_API_GET_MAP,
            spanIds.get(0),
            1,
            null);

        List<SpanId> reqSpanIds = checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_GET_REQUEST,
            spanIds.get(0),
            2,
            null);

        // Futute 1.
        List<SpanId> dhtGetFutSpanIds1 = checkSpan(
            CACHE_API_DHT_GET_FUTURE,
            reqSpanIds.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_GET_MAP,
            dhtGetFutSpanIds1.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_GET_RESPONSE,
            reqSpanIds.get(0),
            1,
            null);

        // Futute 2.
        List<SpanId> dhtGetFutSpanIds2 = checkSpan(
            CACHE_API_DHT_GET_FUTURE,
            reqSpanIds.get(1),
            1,
            null);

        checkSpan(
            CACHE_API_GET_MAP,
            dhtGetFutSpanIds2.get(0),
            1,
            null);

        checkSpan(
            CACHE_API_NEAR_PROCESS_ATOMIC_GET_RESPONSE,
            reqSpanIds.get(1),
            1,
            null);
    }
}
