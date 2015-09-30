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
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.twitter.hbc.core.HttpHosts;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test for {@link TwitterStreamer}. Tests Public Status streaming API https://dev.twitter.com/streaming/public.
 */
public class IgniteTwitterStreamerTest extends GridCommonAbstractTest {

    private static final int CACHE_ENTRY_COUNT = 100;

    /** mocked api in embedded server */
    private static final String MOCK_TWEET_PATH = "/tweet/mock";

    /** sample tweet */
    private static final String tweet = "{\"id\":647375831971590144,\"text\":\"sample tweet to test streamer\"}\n";

    /** Constructor. */
    public IgniteTwitterStreamerTest() {
        super(true);
    }

    @Rule
    /** embedded mock HTTP server for Twitter API */
    public final WireMockRule wireMockRule = new WireMockRule();
    public final WireMockServer mockServer = new WireMockServer(); //starts server on 8080 port

    @Before @SuppressWarnings("unchecked")
    public void beforeTest() throws Exception {
        grid().<String, String>getOrCreateCache(defaultCacheConfiguration());

        mockServer.start();

        stubFor(get(urlMatching("/1.1" + MOCK_TWEET_PATH + ".*")).willReturn(aResponse().
                withHeader("Content-Type", "text/plain").withBody(tweet.length() + "\n" + tweet)));
    }

    @After
    public void afterTest() throws Exception {
        grid().cache(null).clear();
        mockServer.stop();
    }

    public void testStatusesFilterEndpointOAuth1() throws Exception {
        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            TwitterStreamer<String, String> streamer = newStreamerInstance(dataStreamer);

            streamer.setHosts(new HttpHosts("http://localhost:8080"));
            streamer.setEndpointUrl(MOCK_TWEET_PATH);
            Map<String, String> params = new HashMap<>();
            params.put("track", "apache, twitter");
            streamer.setApiParams(params);

            executeStreamer(streamer);
        }
    }

    private void executeStreamer(TwitterStreamer streamer) throws Exception{
        CacheListener listener = subscribeToPutEvents();

        streamer.start();

        // every second check if any data is streamed to cache, for 10s max.
        CountDownLatch latch = listener.getLatch();
        for(int i = 0; i < 10 || latch.getCount() == CACHE_ENTRY_COUNT; i++){
            latch.await(1, TimeUnit.SECONDS);
        }
        unsubscribeToPutEvents(listener);

        assertTrue(latch.getCount() < CACHE_ENTRY_COUNT);
        Status status = TwitterObjectFactory.createStatus(tweet);
        String cachedValue = grid().<String, String>cache(null).get(String.valueOf(status.getId()));
        assertTrue(cachedValue != null);
        assertTrue(cachedValue.equals(status.getText()));

        streamer.stop();
    }

    private CacheListener subscribeToPutEvents() {
        Ignite ignite = grid();

        // Listen to cache PUT events and expect as many as messages as test data items
        CacheListener listener = new CacheListener();
        ignite.events(ignite.cluster().forCacheNodes(null)).localListen(listener, EVT_CACHE_OBJECT_PUT);
        return listener;
    }

    private void unsubscribeToPutEvents(CacheListener listener) {
        Ignite ignite = grid();

        ignite.events(ignite.cluster().forCacheNodes(null)).stopLocalListen(listener, EVT_CACHE_OBJECT_PUT);
    }

    private TwitterStreamer<String, String> newStreamerInstance(IgniteDataStreamer<String, String> dataStreamer) {
        OAuthSettings oAuthSettings = new OAuthSettings("<dummy>", "<dummy>", "<dummy>", "<dummy>");

        TwitterStreamer<String, String> streamer = new TwitterStreamer<>(oAuthSettings);
        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);
        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(10);

        return streamer;
    }

    private class CacheListener implements IgnitePredicate<CacheEvent> {

        private CountDownLatch latch;

        public CacheListener(){
            latch = new CountDownLatch(CACHE_ENTRY_COUNT);
        }

        public CountDownLatch getLatch() {
            return latch;
        }

        @Override
        public boolean apply(CacheEvent evt) {
            latch.countDown();
            return true;
        }
    }
}
