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
import com.google.common.collect.Maps;
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
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Streamer that consumes from a Twitter Streaming API and feeds transformed key-value pairs,
 * by default <tweetId, text>, into an {@link IgniteDataStreamer} instance.
 * <p>
 * This streamer uses https://dev.twitter.com/streaming API and supports Public API, User Streams,
 * Site Streams and Firehose.
 * <p>
 * You can also provide a {@link TweetTransformer} to convert the incoming message into cache entries to override
 * default transformer.
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
    private IgniteLogger log;

    /**
     * The message transformer that converts an incoming Tweet into cache entries.
     * If not provided default transformer will be used.
     */
    private TweetTransformer<K, V> transformer;

    /** Twitter Streaming API params. See https://dev.twitter.com/streaming/overview/request-parameters */
    private Map<String, String> apiParams = Collections.EMPTY_MAP;

    /** Twitter streaming API endpoint example, '/statuses/filter.json' or '/statuses/firehose.json' */
    private String endpointUrl;

    /**
     * Twitter streaming Host. This is paired with endpoint in @link{com.twitter.hbc.core.HttpHosts} constants.
     * This is for configuring mock or enterprise endpoint.
     */
    private HttpHosts hosts;

    /** OAuth params holder */
    private OAuthSettings oAuthSettings;

    /** shared variable to communicate/signal that streamer is already running or can be started */
    private volatile boolean running = false;

    /** Size of buffer for streaming, as for some tracking terms traffic can be low and for others high,
     * this is configurable */
    private Integer bufferCapacity = 100000;

    /** Twitter streaming client (Twitter HBC) to interact with stream */
    private Client client;

    /** Process stream asynchronously */
    private final ExecutorService tweetStreamProcessor = Executors.newSingleThreadExecutor();

    /** Param key name constant for Site streaming */
    private final String SITE_USER_ID_KEY = "follow";

    public TwitterStreamer(OAuthSettings oAuthSettings){
        this.oAuthSettings = oAuthSettings;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IgniteException {
        if (running)
            throw new IgniteException("Attempted to start an already started Twitter Streamer");
        running = true;

        validate();

        log = getIgnite().log();

        BlockingQueue<String> tweetQueue = new LinkedBlockingQueue<String>(bufferCapacity);

        client = getStreamingEndpoint(tweetQueue);

        client.connect();

        if(transformer == null){
            transformer = new DefaultTweetTransformer();
        }
        Callable<Boolean> task = new Callable<Boolean>() {

            @Override
            public Boolean call(){
                while (!client.isDone() && running) {
                    try{
                        String tweet = tweetQueue.take();
                        Map<K, V> value = transformer.apply(tweet);
                        if (value != null)
                            getStreamer().addData(value);
                    }catch (InterruptedException e){
                        //suppressing interrupt. To stop streaming call stop method.
                    }
                }
                return running;
            }
        };
        tweetStreamProcessor.submit(task);
    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {
        running = false;

        tweetStreamProcessor.shutdown();
        try {
            if (!tweetStreamProcessor.awaitTermination(5000, TimeUnit.MILLISECONDS))
                if (log.isDebugEnabled())
                    log.debug("Timed out waiting for consumer threads to shut down, exiting uncleanly.");
        } catch (InterruptedException e) {
            if (log.isDebugEnabled())
                log.debug("Interrupted during shutdown, exiting uncleanly.");
        }
        client.stop();
    }

    private void validate(){
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.notNull(endpointUrl, "Twitter Streaming API endpoint");

        if(endpointUrl.equalsIgnoreCase(SitestreamEndpoint.PATH)){
            String followParam = apiParams.get(SITE_USER_ID_KEY);
            A.ensure(followParam != null && followParam.matches("^(\\d+,? ?)+$"),
                    "Site streaming endpoint must provide 'follow' param with value as comma separated numbers");
        }
    }

    private Client getStreamingEndpoint(BlockingQueue<String> tweetQueue){
        StreamingEndpoint endpoint;
        Authentication authentication = new OAuth1(oAuthSettings.getConsumerKey(), oAuthSettings.getConsumerSecret(),
                oAuthSettings.getAccessToken(), oAuthSettings.getAccessTokenSecret());

        switch (endpointUrl.toLowerCase()){
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
                for(String follower : Splitter.on(',').trimResults().omitEmptyStrings().split(follow)){
                    followers.add(Long.valueOf(follower));
                }
                endpoint = new SitestreamEndpoint(followers);
                hosts = HttpHosts.SITESTREAM_HOST;
                break;
            default:
                endpoint = new DefaultStreamingEndpoint(endpointUrl, HttpConstants.HTTP_GET, false);
                if(hosts == null){
                    hosts = HttpHosts.STREAM_HOST;
                }
        }
        for(Map.Entry<String, String> entry : apiParams.entrySet()){
            endpoint.addPostParameter(entry.getKey(), entry.getValue());
        }
        ClientBuilder builder = new ClientBuilder()
                .name("Ignite-Twitter-Client")
                .hosts(hosts)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(tweetQueue));

        Client client = builder.build();

        return client;
    }

    private static class DefaultTweetTransformer implements TweetTransformer {

        @Override
        public Map<String, String> apply(String tweet) {
            try{
                Status status = TwitterObjectFactory.createStatus(tweet);
                Map<String, String> value = Maps.newHashMap();
                value.put(String.valueOf(status.getId()), status.getText());
                return value;
            }catch (TwitterException e){
                //suppress exception. If failed to transform, will return empty map.
            }
            return Collections.EMPTY_MAP;
        }
    }

    public void setTransformer(TweetTransformer<K, V> transformer) {
        this.transformer = transformer;
    }

    public void setApiParams(Map<String, String> apiParams) {
        this.apiParams = apiParams;
    }

    public void setEndpointUrl(String endpointUrl) {
        this.endpointUrl = endpointUrl;
    }

    public void setBufferCapacity(Integer bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
    }

    /** only for mock and enterprise endpoint. Otherwise mapped with endpoint url */
    public void setHosts(HttpHosts hosts) {
        this.hosts = hosts;
    }
}
