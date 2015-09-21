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

import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.twitter.hbc.core.endpoint.*;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;


import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;

/**
 * Test for {@link TwitterStreamer}. Tests Public Status streaming API https://dev.twitter.com/streaming/public.
 *
 * @author Lalit Jha
 */
public class IgniteTwitterStreamerTest extends GridCommonAbstractTest {

    private static final int CACHE_ENTRY_COUNT = 100;

    private ResourceBundle twitterLogin;

    /** Constructor. */
    public IgniteTwitterStreamerTest() {
        super(true);
    }

    @Before @SuppressWarnings("unchecked")
    public void beforeTest() throws Exception {
        grid().<Integer, String>getOrCreateCache(defaultCacheConfiguration());
        twitterLogin = ResourceBundle.getBundle("twitter");
    }

    @After
    public void afterTest() throws Exception {
        grid().cache(null).clear();

    }

    public void testStatusesFilterEndpointOAuth1() throws Exception {
        try (IgniteDataStreamer<String, String> dataStreamer = grid().dataStreamer(null)) {
            TwitterStreamer<String, String> streamer = newStreamerInstance(dataStreamer);
            setAuthProperties(streamer, OAuth1.class);
            streamer.setEndpointUrl(StatusesFilterEndpoint.PATH);
            Map<String, String> params = new HashMap<>();
            params.put("track", "apache, twitter");
            streamer.setApiParams(params);
            executeStreamer(streamer);
        }
    }

    private void executeStreamer(TwitterStreamer streamer) throws Exception{
        CacheListener listener = subscribeToPutEvents();

        streamer.start();

        // all cache PUT events received in 10 seconds, wait 10 more seconds as Twitter API can take time
        CountDownLatch latch = listener.getLatch();
        latch.await(10, TimeUnit.SECONDS);
        if(latch.getCount() == CACHE_ENTRY_COUNT){
            latch.await(10, TimeUnit.SECONDS);
        }
        unsubscribeToPutEvents(listener);
        assertTrue(latch.getCount() < CACHE_ENTRY_COUNT);
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

        TwitterStreamer<String, String> streamer = new TwitterStreamer<>();
        streamer.setIgnite(grid());
        streamer.setStreamer(dataStreamer);

        dataStreamer.allowOverwrite(true);
        dataStreamer.autoFlushFrequency(10);
        return streamer;
    }

    private void setAuthProperties(TwitterStreamer streamer, Class<? extends Authentication> authScheme){
        if(authScheme.isAssignableFrom(OAuth1.class)){
            streamer.setConsumerKey(twitterLogin.getString("consumerKey"));
            streamer.setConsumerSecret(twitterLogin.getString("consumerSecret"));
            streamer.setAccessToken(twitterLogin.getString("accessToken"));
            streamer.setAccessTokenSecret(twitterLogin.getString("accessTokenSecret"));
        }
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
            log.debug(String.valueOf(latch.getCount()));
            return true;
        }

    }

}
