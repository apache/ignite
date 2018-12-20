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

package org.apache.ignite.stream.twitter;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockClassRule;
import com.twitter.hbc.core.HttpHosts;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import junit.framework.TestResult;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.runners.JUnit38ClassRunner;
import org.junit.runner.RunWith;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test for {@link TwitterStreamer}. Tests Public Status streaming API https://dev.twitter.com/streaming/public.
 */
@RunWith(JUnit38ClassRunner.class) // TODO IGNITE-10739 migrate to JUnit 4.
public class IgniteTwitterStreamerTest extends GridCommonAbstractTest {
    /** Cache entries count. */
    private static final int CACHE_ENTRY_COUNT = 100;

    /** Mocked api in embedded server. */
    private static final String MOCK_TWEET_PATH = "/tweet/mock";

    /** Sample tweet. */
    private static final String tweet = "{\"id\":647375831971590144,\"text\":\"sample tweet to test streamer\"}\n";

    /** Constructor. */
    public IgniteTwitterStreamerTest() {
        super(true);
    }

    /** See <a href="http://wiremock.org/docs/junit-rule/">The JUnit 4.x Rule </a> */
    @ClassRule
    public static WireMockClassRule wireMockClsRule = new WireMockClassRule(WireMockConfiguration.DYNAMIC_PORT);

    /** Embedded mock HTTP server's for Twitter API rule. */
    @Rule
    public WireMockClassRule wireMockRule = wireMockClsRule;

    /** Embedded mock HTTP server for Twitter API. */
    public final WireMockServer mockSrv = new WireMockServer(); //Starts server on 8080 port.

    /**
     * {@inheritDoc}
     * <p>
     * Fallback to TestCase functionality.</p>
     */
    @Override public int countTestCases() {
        return countTestCasesFallback();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Fallback to TestCase functionality.</p>
     */
    @Override public void run(TestResult res) {
        runFallback(res);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Fallback to TestCase functionality.</p>
     */
    @Override public String getName() {
        return getNameFallback();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10_000;
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() {
        grid().getOrCreateCache(defaultCacheConfiguration());

        mockSrv.start();

        stubFor(get(urlMatching("/1.1" + MOCK_TWEET_PATH + ".*")).willReturn(aResponse().
            withHeader("Content-Type", "text/plain").withBody(tweet.length() + "\n" + tweet)));
    }

    /** {@inheritDoc} */
    @Override public void afterTest() {
        stopAllGrids();

        mockSrv.stop();
    }

    /**
     * @throws Exception Test exception.
     */
    @Test
    public void testStatusesFilterEndpointOAuth1() throws Exception {
        try (IgniteDataStreamer<Long, String> dataStreamer = grid().dataStreamer(DEFAULT_CACHE_NAME)) {
            TwitterStreamerImpl streamer = newStreamerInstance(dataStreamer);

            Map<String, String> params = new HashMap<>();

            params.put("track", "apache, twitter");
            params.put("follow", "3004445758");// @ApacheIgnite id.

            streamer.setApiParams(params);
            streamer.setEndpointUrl(MOCK_TWEET_PATH);
            streamer.setHosts(new HttpHosts("http://localhost:8080"));
            streamer.setThreadsCount(8);

            executeStreamer(streamer);
        }
    }

    /**
     * @param streamer Twitter streamer.
     * @throws InterruptedException Test exception.
     * @throws TwitterException Test exception.
     */
    private void executeStreamer(TwitterStreamer streamer) throws InterruptedException, TwitterException {
        // Checking streaming.

        CacheListener lsnr = subscribeToPutEvents();

        streamer.start();

        try {
            streamer.start();

            fail("Successful start of already started Twitter Streamer");
        }
        catch (IgniteException ignored) {
            // No-op.
        }

        CountDownLatch latch = lsnr.getLatch();

        // Enough tweets was handled in 10 seconds. Limited by test's timeout.
        latch.await();

        unsubscribeToPutEvents(lsnr);

        streamer.stop();

        try {
            streamer.stop();

            fail("Successful stop of already stopped Twitter Streamer");
        }
        catch (IgniteException ignored) {
            // No-op.
        }

        // Checking cache content after streaming finished.

        Status status = TwitterObjectFactory.createStatus(tweet);

        IgniteCache<Long, String> cache = grid().cache(DEFAULT_CACHE_NAME);

        String cachedVal = cache.get(status.getId());

        // Tweet successfully put to cache.
        assertTrue(cachedVal != null && cachedVal.equals(status.getText()));

        // Same tweets does not produce duplicate entries.
        assertEquals(1, cache.size());
    }

    /**
     * @return Cache listener.
     */
    private CacheListener subscribeToPutEvents() {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items.
        CacheListener lsnr = new CacheListener();

        ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).localListen(lsnr, EVT_CACHE_OBJECT_PUT);

        return lsnr;
    }

    /**
     * @param lsnr Cache listener.
     */
    private void unsubscribeToPutEvents(CacheListener lsnr) {
        Ignite ignite = grid();

        ignite.events(ignite.cluster().forCacheNodes(DEFAULT_CACHE_NAME)).stopLocalListen(lsnr, EVT_CACHE_OBJECT_PUT);
    }

    /**
     * @param dataStreamer Ignite Data Streamer.
     * @return Twitter Streamer.
     */
    private TwitterStreamerImpl newStreamerInstance(IgniteDataStreamer<Long, String> dataStreamer) {
        OAuthSettings oAuthSettings = new OAuthSettings("<dummy>", "<dummy>", "<dummy>", "<dummy>");

        TwitterStreamerImpl streamer = new TwitterStreamerImpl(oAuthSettings);

        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(10);

        return streamer;
    }

    /**
     * Listener.
     */
    private static class CacheListener implements IgnitePredicate<CacheEvent> {

        /** */
        private final CountDownLatch latch = new CountDownLatch(CACHE_ENTRY_COUNT);

        /**
         * @return Latch.
         */
        public CountDownLatch getLatch() {
            return latch;
        }

        /**
         * @param evt Cache Event.
         * @return {@code true}.
         */
        @Override public boolean apply(CacheEvent evt) {
            latch.countDown();

            return true;
        }
    }
}
