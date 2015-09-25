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
    String tweet = "{\"created_at\":\"Fri Sep 25 11:43:20 +0000 2015\",\"id\":647375831971590144,\"id_str\":\"647375831971590144\",\"text\":\"\\u041f\\u044f\\u0442\\u043d\\u0438\\u0446\\u0430 \\u0442\\u0430\\u043a \\u043f\\u044f\\u0442\\u043d\\u0438\\u0446\\u0430\\ud83d\\udc4d\\ud83c\\udffc\\ud83d\\udc4d\\ud83c\\udffc\\ud83d\\udc4d\\ud83c\\udffc\\ud83d\\ude04 (@ \\u041f\\u0440\\u043e\\u0441\\u043f\\u0435\\u043a\\u0442 \\u041b\\u044e\\u0431\\u0438\\u043c\\u043e\\u0432\\u0430 in Minsk) https:\\/\\/t.co\\/JcQrQtQcV6\",\"source\":\"\\u003ca href=\\\"http:\\/\\/foursquare.com\\\" rel=\\\"nofollow\\\"\\u003eFoursquare\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":953599657,\"id_str\":\"953599657\",\"name\":\"K A T Y A\",\"screen_name\":\"Lady_Kolber\",\"location\":\"\\u041c\\u0438\\u043d\\u0441\\u043a\",\"url\":null,\"description\":null,\"protected\":false,\"verified\":false,\"followers_count\":98,\"friends_count\":62,\"listed_count\":8,\"favourites_count\":818,\"statuses_count\":7667,\"created_at\":\"Sat Nov 17 13:41:18 +0000 2012\",\"utc_offset\":10800,\"time_zone\":\"Baghdad\",\"geo_enabled\":true,\"lang\":\"ru\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"0099B9\",\"profile_background_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000159769086\\/x2fWF2nQ.jpeg\",\"profile_background_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_background_images\\/378800000159769086\\/x2fWF2nQ.jpeg\",\"profile_background_tile\":true,\"profile_link_color\":\"0099B9\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"95E8EC\",\"profile_text_color\":\"3C3940\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/636313079224139776\\/z93GNw7-_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/636313079224139776\\/z93GNw7-_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/953599657\\/1440236650\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":{\"type\":\"Point\",\"coordinates\":[53.863376,27.458605]},\"coordinates\":{\"type\":\"Point\",\"coordinates\":[27.458605,53.863376]},\"place\":{\"id\":\"333a5811d6b0c1cb\",\"url\":\"https:\\/\\/api.twitter.com\\/1.1\\/geo\\/id\\/333a5811d6b0c1cb.json\",\"place_type\":\"country\",\"name\":\"\\u0411\\u0435\\u043b\\u0430\\u0440\\u0443\\u0441\\u044c\",\"full_name\":\"\\u0411\\u0435\\u043b\\u0430\\u0440\\u0443\\u0441\\u044c\",\"country_code\":\"BY\",\"country\":\"Belarus'\",\"bounding_box\":{\"type\":\"Polygon\",\"coordinates\":[[[23.179217,51.2626423],[23.179217,56.1717339],[32.7769812,56.1717339],[32.7769812,51.2626423]]]},\"attributes\":{}},\"contributors\":null,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[],\"trends\":[],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/JcQrQtQcV6\",\"expanded_url\":\"https:\\/\\/www.swarmapp.com\\/c\\/jAFkmVxZnFQ\",\"display_url\":\"swarmapp.com\\/c\\/jAFkmVxZnFQ\",\"indices\":[58,81]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"ru\",\"timestamp_ms\":\"1443181400660\"}\n";

    /** Constructor. */
    public IgniteTwitterStreamerTest() {
        super(true);
    }

    @Rule
    /** embedded mock HTTP server for Twitter API */
    public WireMockRule wireMockRule = new WireMockRule();
    WireMockServer mockServer = new WireMockServer(); //starts server on 8080 port

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

        // all cache PUT events received in 10 seconds, wait 10 more seconds as Twitter API can take time
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
