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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.HttpConstants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint;
import com.twitter.hbc.core.endpoint.SitestreamEndpoint;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StatusesFirehoseEndpoint;
import com.twitter.hbc.core.endpoint.StatusesSampleEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import com.twitter.hbc.core.endpoint.UserstreamEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamAdapter;

/**
 * Streamer that consumes from a Twitter Streaming API and feeds transformed key-value pairs,
 * by default <tweetId, text>, into an {@link IgniteDataStreamer} instance.
 * <p>
 * This streamer uses https://dev.twitter.com/streaming API and supports Public API, User Streams,
 * Site Streams and Firehose.
 * <p>
 * This Streamer features:
 * <ul>
 *     <li>Supports OAuth1 authentication scheme.
 *     <br/> BasicAuth not supported by Streaming API https://dev.twitter.com/streaming/overview/connecting</li>
 *     <li>Provide all params in apiParams map. https://dev.twitter.com/streaming/overview/request-parameters</li>
 * </ul>
 */
public class TwitterStreamer<K, V> extends StreamAdapter<String, K, V> {
    /** Logger. */
    protected IgniteLogger log;

    /** Threads count used to transform tweets. */
    private int threadsCount = 1;

    /** Twitter Streaming API params. See https://dev.twitter.com/streaming/overview/request-parameters */
    private Map<String, String> apiParams;

    /** Twitter streaming API endpoint example, '/statuses/filter.json' or '/statuses/firehose.json' */
    private String endpointUrl;

    /** OAuth params holder */
    private OAuthSettings oAuthSettings;

    /** shared variable to communicate/signal that streamer is already running or can be started */
    private final AtomicInteger running = new AtomicInteger();

    /**
     * Size of buffer for streaming, as for some tracking terms traffic can be low and for others high, this is
     * configurable
     */
    private Integer bufferCapacity = 100000;

    /** Twitter streaming client (Twitter HBC) to interact with stream */
    private Client client;

    /** Process stream asynchronously */
    private ExecutorService tweetStreamProcessor;

    /** Param key name constant for Site streaming */
    private final String SITE_USER_ID_KEY = "follow";

    /**
     * @param oAuthSettings OAuth Settings
     */
    public TwitterStreamer(OAuthSettings oAuthSettings) {
        this.oAuthSettings = oAuthSettings;
    }

    /**
     * Starts streamer.
     */
    public void start() {
        if (!running.compareAndSet(0, 1))
            throw new IgniteException("Attempted to start an already started Twitter Streamer");

        validateConfig();

        log = getIgnite().log();

        final BlockingQueue<String> tweetQueue = new LinkedBlockingQueue<>(bufferCapacity);

        client = getClient(tweetQueue);

        client.connect();

        tweetStreamProcessor = Executors.newFixedThreadPool(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            Callable<Boolean> task = new Callable<Boolean>() {

                @Override
                public Boolean call() {
                    while (true) {
                        try {
                            String tweet = tweetQueue.take();

                            addMessage(tweet);
                        }
                        catch (InterruptedException e) {
                            U.warn(log, "Tweets transformation was interrupted", e);

                            return true;
                        }
                    }
                }
            };

            tweetStreamProcessor.submit(task);
        }
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        if (running.get() == 0)
            throw new IgniteException("Attempted to stop an already stopped Twitter Streamer");

        tweetStreamProcessor.shutdownNow();

        client.stop();

        running.compareAndSet(1, 0);
    }

    /**
     * Validates config at start.
     */
    protected void validateConfig() {
        A.notNull(getStreamer(), "Streamer");
        A.notNull(getIgnite(), "Ignite");
        A.notNull(endpointUrl, "Twitter Streaming API endpoint");

        A.ensure(getSingleTupleExtractor() != null || getMultipleTupleExtractor() != null, "Twitter extractor");

        String followParam = apiParams.get(SITE_USER_ID_KEY);

        A.ensure(followParam != null && followParam.matches("^(\\d+,? ?)+$"),
            "Site streaming endpoint must provide 'follow' param with value as comma separated numbers");
    }

    /**
     * @param tweetQueue Tweet queue.
     * @return Client.
     */
    protected Client getClient(BlockingQueue<String> tweetQueue) {
        StreamingEndpoint endpoint;

        HttpHosts hosts;

        switch (endpointUrl.toLowerCase()) {
            case StatusesFilterEndpoint.PATH:
                endpoint = new StatusesFilterEndpoint();

                hosts = HttpHosts.STREAM_HOST;

                break;
            case StatusesFirehoseEndpoint.PATH:
                endpoint = new StatusesFirehoseEndpoint();

                hosts = HttpHosts.STREAM_HOST;

                break;
            case StatusesSampleEndpoint.PATH:
                endpoint = new StatusesSampleEndpoint();

                hosts = HttpHosts.STREAM_HOST;

                break;
            case UserstreamEndpoint.PATH:
                endpoint = new UserstreamEndpoint();

                hosts = HttpHosts.USERSTREAM_HOST;

                break;
            case SitestreamEndpoint.PATH:
                String follow = apiParams.remove(SITE_USER_ID_KEY);

                List<Long> followers = Lists.newArrayList();

                for (String follower : Splitter.on(',').trimResults().omitEmptyStrings().split(follow))
                    followers.add(Long.valueOf(follower));

                endpoint = new SitestreamEndpoint(followers);

                hosts = HttpHosts.SITESTREAM_HOST;

                break;
            default:
                endpoint = new DefaultStreamingEndpoint(endpointUrl, HttpConstants.HTTP_GET, false);

                hosts = HttpHosts.STREAM_HOST;

        }

        for (Map.Entry<String, String> entry : apiParams.entrySet()) {
            endpoint.addPostParameter(entry.getKey(), entry.getValue());
        }

        return buildClient(tweetQueue, hosts, endpoint);
    }

    /**
     * @param tweetQueue tweet Queue.
     * @param hosts Hostes.
     * @param endpoint Endpoint.
     * @return Client.
     */
    protected Client buildClient(BlockingQueue<String> tweetQueue, HttpHosts hosts, StreamingEndpoint endpoint) {
        Authentication authentication = new OAuth1(oAuthSettings.getConsumerKey(), oAuthSettings.getConsumerSecret(),
            oAuthSettings.getAccessToken(), oAuthSettings.getAccessTokenSecret());

        ClientBuilder builder = new ClientBuilder()
            .name("Ignite-Twitter-Client")
            .hosts(hosts)
            .authentication(authentication)
            .endpoint(endpoint)
            .processor(new StringDelimitedProcessor(tweetQueue));

        return builder.build();
    }

    /**
     * Sets API Params.
     *
     * @param apiParams API Params.
     */
    public void setApiParams(Map<String, String> apiParams) {
        this.apiParams = apiParams;
    }

    /**
     * Sets Endpoint URL.
     *
     * @param endpointUrl Endpoint URL.
     */
    public void setEndpointUrl(String endpointUrl) {
        this.endpointUrl = endpointUrl;
    }

    /**
     * Sets Buffer capacity.
     *
     * @param bufferCapacity Buffer capacity.
     */
    public void setBufferCapacity(Integer bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
    }

    /**
     * Sets Threads count.
     *
     * @param threadsCount Threads count.
     */
    public void setThreadsCount(int threadsCount) {
        this.threadsCount = threadsCount;
    }
}
