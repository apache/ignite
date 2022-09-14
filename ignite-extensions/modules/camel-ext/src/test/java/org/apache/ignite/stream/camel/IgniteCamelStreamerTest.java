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

package org.apache.ignite.stream.camel;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.ServiceStatus;
import org.apache.camel.component.properties.PropertiesComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.engine.DefaultProducerTemplate;
import org.apache.camel.support.LifecycleStrategySupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test class for {@link CamelStreamer}.
 */
public class IgniteCamelStreamerTest extends GridCommonAbstractTest {
    /** The test data. */
    private static final Map<Integer, String> TEST_DATA = new HashMap<>();

    /** The producer template used to publish data to the endpoint. */
    private DefaultProducerTemplate producerTemplate;

    /** The Camel streamer currently under test. */
    private CamelStreamer<Integer, String> streamer;

    /** The Ignite data streamer. */
    private IgniteDataStreamer<Integer, String> dataStreamer;

    /** URL where the REST service will be exposed. */
    private String url;

    /** The UUID of the currently active remote listener. */
    private UUID remoteLsnr;

    // Initialize the test data.
    static {
        for (int i = 0; i < 100; i++)
            TEST_DATA.put(i, "v" + i);
    }

    /** Constructor. */
    public IgniteCamelStreamerTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_ALL);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());

        // find an available local port
        try (ServerSocket ss = new ServerSocket(0)) {
            int port = ss.getLocalPort();

            url = "http://localhost:" + port + "/ignite";
        }

        DefaultCamelContext context = new DefaultCamelContext();
        // create Camel streamer
        dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME);
        streamer = createCamelStreamer(dataStreamer);
        streamer.setCamelContext(context);
        producerTemplate = new DefaultProducerTemplate(context);
        producerTemplate.start();
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        try {
            streamer.stop();
        }
        catch (Exception ignored) {
            // ignore if already stopped
        }

        dataStreamer.close();

        grid().cache(DEFAULT_CACHE_NAME).clear();
    }

    /**
     * @throws Exception
     */
    @Test
    public void testSendOneEntryPerMessage() throws Exception {
        streamer.setSingleTupleExtractor(singleTupleExtractor());

        // Subscribe to cache PUT events.
        CountDownLatch latch = subscribeToPutEvents(50);

        // Action time.
        streamer.start();

        // Send messages.
        sendMessages(0, 50, false);

        // Assertions.
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testMultipleEntriesInOneMessage() throws Exception {
        streamer.setMultipleTupleExtractor(multipleTupleExtractor());

        // Subscribe to cache PUT events.
        CountDownLatch latch = subscribeToPutEvents(50);

        // Action time.
        streamer.start();

        // Send messages.
        sendMessages(0, 50, true);

        // Assertions.
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testResponseProcessorIsCalled() throws Exception {
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setResponseProcessor(new Processor() {
            @Override public void process(Exchange exchange) throws Exception {
                exchange.getOut().setBody("Foo bar");
            }
        });

        // Subscribe to cache PUT events.
        CountDownLatch latch = subscribeToPutEvents(50);

        // Action time.
        streamer.start();

        // Send messages.
        List<String> responses = sendMessages(0, 50, false);

        for (String r : responses)
            assertEquals("Foo bar", r);

        // Assertions.
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserSpecifiedCamelContext() throws Exception {

        CamelContext context = streamer.getCamelContext();

        final AtomicInteger cnt = new AtomicInteger();
        context.setTracing(true);
        context.addLifecycleStrategy(new LifecycleStrategySupport() {
            @Override public void onEndpointAdd(Endpoint endpoint) {
                cnt.incrementAndGet();
            }
        });

        streamer.setSingleTupleExtractor(singleTupleExtractor());

        // Subscribe to cache PUT events.
        CountDownLatch latch = subscribeToPutEvents(50);

        // Action time.
        streamer.start();

        // Send messages.
        sendMessages(0, 50, false);

        // Assertions.
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);
        assertTrue(cnt.get() > 0);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testUserSpecifiedCamelContextWithPropertyPlaceholders() throws Exception {
        // Create a CamelContext with a custom property placeholder.
        CamelContext context = new DefaultCamelContext();

        PropertiesComponent pc = new PropertiesComponent("camel.test.properties");

        context.setPropertiesComponent(pc);

        // Replace the context path in the test URL with the property placeholder.
        url = url.replaceAll("/ignite", "{{test.contextPath}}");

        // Recreate the Camel streamer with the new URL.
        streamer = createCamelStreamer(dataStreamer);

        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setCamelContext(context);

        producerTemplate = new DefaultProducerTemplate(context);
        producerTemplate.start();

        // Subscribe to cache PUT events.
        CountDownLatch latch = subscribeToPutEvents(50);

        // Action time.
        streamer.start();

        // Before sending the messages, get the actual URL after the property placeholder was resolved,
        // stripping the jetty: prefix from it.
        url = streamer.getCamelContext().getEndpoints().iterator().next().getEndpointUri().replaceAll("jetty:", "");

        // Send messages.
        sendMessages(0, 50, false);

        // Assertions.
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        assertCacheEntriesLoaded(50);
    }

    /**
     * @throws Exception
     */
    @Test
    public void testInvalidEndpointUri() throws Exception {
        streamer.setSingleTupleExtractor(singleTupleExtractor());
        streamer.setEndpointUri("abc");

        // Action time.
        try {
            streamer.start();
            fail("Streamer started; should have failed.");
        }
        catch (IgniteException ignored) {
            CamelContext context = streamer.getCamelContext();
            ServiceStatus status = context.getStatus();
            assertEquals("streamer should be in a stopped state, instead is " + status, status, ServiceStatus.Stopped);
            assertEquals("endpoint registry has > 0 elements, size: " + context.getEndpointRegistry().size(),
                context.getEndpointRegistry().size(), 0);
        }
    }

    /**
     * Creates a Camel streamer.
     */
    private CamelStreamer<Integer, String> createCamelStreamer(IgniteDataStreamer<Integer, String> dataStreamer) {
        CamelStreamer<Integer, String> streamer = new CamelStreamer<>();

        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);
        streamer.setEndpointUri("jetty:" + url);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(1);

        return streamer;
    }

    /**
     * @throws Exception
     * @return HTTP response payloads.
     */
    private List<String> sendMessages(int fromIdx, int cnt, boolean singleMessage) throws Exception {
        List<String> responses = Lists.newArrayList();

        if (singleMessage) {
            StringBuilder sb = new StringBuilder();

            for (int i = fromIdx; i < fromIdx + cnt; i++)
                sb.append(i).append(",").append(TEST_DATA.get(i)).append("\n");

            Exchange out = producerTemplate.send(url, exchange -> exchange.getIn().setBody(sb));

            if (out.getException() != null)
                throw out.getException();

            responses.add(out.getMessage().toString());
        }
        else {
            for (int i = fromIdx; i < fromIdx + cnt; i++) {
                String payload = i + "," + TEST_DATA.get(i);

                Exchange out = producerTemplate.send(url, exchange -> exchange.getIn().setBody(payload));

                if (out.getException() != null)
                    throw out.getException();

                responses.add(out.getMessage().getBody(String.class));
            }
        }

        return responses;
    }

    /**
     * Returns a {@link StreamSingleTupleExtractor} for testing.
     */
    private static StreamSingleTupleExtractor<Exchange, Integer, String> singleTupleExtractor() {
        return new StreamSingleTupleExtractor<Exchange, Integer, String>() {
            @Override public Map.Entry<Integer, String> extract(Exchange exchange) {
                List<String> s = Splitter.on(",").splitToList(exchange.getIn().getBody(String.class));

                return new GridMapEntry<>(Integer.parseInt(s.get(0)), s.get(1));
            }
        };
    }

    /**
     * Returns a {@link StreamMultipleTupleExtractor} for testing.
     */
    private static StreamMultipleTupleExtractor<Exchange, Integer, String> multipleTupleExtractor() {
        return new StreamMultipleTupleExtractor<Exchange, Integer, String>() {
            @Override public Map<Integer, String> extract(Exchange exchange) {
                final Map<String, String> map = Splitter.on("\n")
                    .omitEmptyStrings()
                    .withKeyValueSeparator(",")
                    .split(exchange.getIn().getBody(String.class));

                final Map<Integer, String> answer = new HashMap<>();

                F.forEach(map.keySet(), new IgniteInClosure<String>() {
                    @Override public void apply(String s) {
                        answer.put(Integer.parseInt(s), map.get(s));
                    }
                });

                return answer;
            }
        };
    }

    /**
     * Subscribe to cache put events.
     */
    private CountDownLatch subscribeToPutEvents(int expect) {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items
        final CountDownLatch latch = new CountDownLatch(expect);
        @SuppressWarnings("serial") IgniteBiPredicate<UUID, CacheEvent> callback =
            new IgniteBiPredicate<UUID, CacheEvent>() {
                @Override public boolean apply(UUID uuid, CacheEvent evt) {
                    latch.countDown();

                    return true;
                }
            };

        remoteLsnr = ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME))
            .remoteListen(callback, null, EVT_CACHE_OBJECT_PUT);

        return latch;
    }

    /**
     * Assert a given number of cache entries have been loaded.
     */
    private void assertCacheEntriesLoaded(int cnt) {
        // get the cache and check that the entries are present
        IgniteCache<Integer, String> cache = grid().cache(DEFAULT_CACHE_NAME);

        // for each key from 0 to count from the TEST_DATA (ordered by key), check that the entry is present in cache
        for (Integer key : new ArrayList<>(new TreeSet<>(TEST_DATA.keySet())).subList(0, cnt))
            assertEquals(TEST_DATA.get(key), cache.get(key));

        // assert that the cache exactly the specified amount of elements
        assertEquals(cnt, cache.size(CachePeekMode.ALL));

        // remove the event listener
        grid().events(grid().cluster().forCacheNodes(DEFAULT_CACHE_NAME)).stopRemoteListen(remoteLsnr);
    }

}
