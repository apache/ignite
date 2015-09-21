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

import java.util.*;
import java.util.concurrent.*;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.*;
import com.twitter.hbc.core.endpoint.*;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

/**
 * Streamer that consumes from a Twitter Streaming API and feeds transformed key-value pairs, by default < tweetId, text>,
 * into an {@link IgniteDataStreamer} instance.
 * <p>
 * This streamer uses https://dev.twitter.com/streaming API and supports Public API, User Streams, Site Streams and Firehose.
 * <p>
 * You can also provide a {@link TweetTransformer} to convert the incoming message into cache entries to override
 * default transformer.
 * <p>
 * This Streamer features:
 *
 * <ul>
 *     <li>Supports OAuth1 authentication scheme. <br/> BasicAuth not supported by Streaming API https://dev.twitter.com/streaming/overview/connecting</li>
 *     <li>Provide all params in apiParams map. https://dev.twitter.com/streaming/overview/request-parameters</li>
 * </ul>
 *
 * @author Lalit Jha
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
    private Map<String, String> apiParams = new HashMap<String, String>();

    /** Twitter streaming API endpoint example, '/statuses/filter.json' or '/statuses/firehose.json' */
    private String endpointUrl;

    /** BasicAuth not supported by Streaming API https://dev.twitter.com/streaming/overview/connecting */
    private Class<? extends Authentication> authenticationScheme = OAuth1.class;

    /** OAuth params */
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;

    private volatile boolean stopped;
    private Integer bufferCapacity = 100000;
    private Client client;

    /** Process stream asynchronously */
    private ExecutorService tweetStreamProcesser = Executors.newSingleThreadExecutor();

    private String SITE_USER_ID_KEY = "follow";

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IgniteException {
        if (stopped)
            throw new IgniteException("Attempted to start an already started Twitter Streamer");

        try {
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
                public Boolean call() throws Exception {
                    while (!client.isDone() && !stopped) {
                        String tweet = tweetQueue.take();
                        Map<K, V> value = transformer.apply(tweet);
                        if (value != null){
                            getStreamer().addData(value);
                        }
                    }
                    return stopped;
                }
            };
            tweetStreamProcesser.submit(task);
        }catch (Throwable t) {
            throw new IgniteException("Exception while initializing TwitterStreamer", t);
        }

    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {

        try {
            stopped = true;
            client.stop();
        }catch (Throwable t) {
            throw new IgniteException("Exception while stopping TwitterStreamer", t);
        }finally {
            /** Even if client.stop() fails, this need to be executed */
            if (tweetStreamProcesser != null) {
                tweetStreamProcesser.shutdown();

                try {
                    if (!tweetStreamProcesser.awaitTermination(5000, TimeUnit.MILLISECONDS))
                        if (log.isDebugEnabled())
                            log.debug("Timed out waiting for consumer threads to shut down, exiting uncleanly.");
                } catch (InterruptedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Interrupted during shutdown, exiting uncleanly.");
                }
            }
        }
    }

    private void validate(){
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");

        A.notNull(endpointUrl, "Twitter Streaming API endpoint");
        A.notNull(authenticationScheme, "twitter Streaming API authentication scheme");
        A.ensure(authenticationScheme.isAssignableFrom(OAuth1.class),
                "twitter Streaming API authentication scheme must be OAuth1");
        if(authenticationScheme.isAssignableFrom(OAuth1.class)){
            A.notNull(consumerKey, "OAuth consumer key");
            A.notNull(consumerSecret, "OAuth consumer secret");
            A.notNull(accessToken, "OAuth access token");
            A.notNull(accessTokenSecret, "OAuth access token secret");
        }

        if(endpointUrl.equalsIgnoreCase(SitestreamEndpoint.PATH)){
            String followParam = apiParams.get(SITE_USER_ID_KEY);
            A.ensure(followParam != null && followParam.matches("^(\\d+,? ?)+$"),
                    "Site streaming endpoint must provide 'follow' param with value as comma separated numbers");
        }
    }

    private Client getStreamingEndpoint(BlockingQueue<String> tweetQueue){
        StreamingEndpoint endpoint;
        Authentication authentication = null;

        if(this.endpointUrl.equalsIgnoreCase(StatusesFilterEndpoint.PATH)){
            endpoint = new StatusesFilterEndpoint();
        }else if(this.endpointUrl.equalsIgnoreCase(StatusesFirehoseEndpoint.PATH)){
            endpoint = new StatusesFirehoseEndpoint();
        }else if(this.endpointUrl.equalsIgnoreCase(StatusesSampleEndpoint.PATH)){
            endpoint = new StatusesSampleEndpoint(); //for testing
        }else if(this.endpointUrl.equalsIgnoreCase(UserstreamEndpoint.PATH)){
            endpoint = new UserstreamEndpoint();
        }else if(this.endpointUrl.equalsIgnoreCase(SitestreamEndpoint.PATH)){
            String follow = apiParams.remove(SITE_USER_ID_KEY);
            List<Long> followers = Lists.newArrayList();
            for(String follower : Splitter.on(',').trimResults().omitEmptyStrings().split(follow)){
                followers.add(Long.valueOf(follower));
            }
            endpoint = new SitestreamEndpoint(followers);
        }else {
            endpoint = new DefaultStreamingEndpoint(this.endpointUrl, HttpConstants.HTTP_GET, false);
        }

        for(Map.Entry<String, String> entry : apiParams.entrySet()){
            endpoint.addPostParameter(entry.getKey(), entry.getValue());
        }

        if(authenticationScheme.isAssignableFrom(OAuth1.class)){
            authentication = new OAuth1(consumerKey, consumerSecret, accessToken, accessTokenSecret);
        }
        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
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
            }catch (Exception e){
                //no op
            }
            return Collections.EMPTY_MAP;
        }
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public void setAccessTokenSecret(String accessTokenSecret) {
        this.accessTokenSecret = accessTokenSecret;
    }

    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    public void setConsumerSecret(String consumerSecret) {
        this.consumerSecret = consumerSecret;
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
}
