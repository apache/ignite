/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.ignite.stream.twitter;

import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

/**
 * Long, String (With Mocked server support) implementation of TwitterStreamer.
 */
public class TwitterStreamerImpl extends TwitterStreamer<Long, String> {
    /** Mocked server support. */
    HttpHosts hosts;

    /**
     * @param oAuthSettings OAuth Settings
     */
    public TwitterStreamerImpl(OAuthSettings oAuthSettings) {
        super(oAuthSettings);

        setSingleTupleExtractor(new TwitterStreamSingleTupleExtractorImpl());
    }

    /**
     * @param hosts hosts.
     */
    public void setHosts(HttpHosts hosts) {
        this.hosts = hosts;
    }

    /** {@inheritDoc} */
    @Override protected Client buildClient(BlockingQueue<String> tweetQueue, HttpHosts hosts,
        StreamingEndpoint endpoint) {
        return super.buildClient(tweetQueue, this.hosts, endpoint);
    }

    /**
     * Long, String Tweet Single Tuple Extractor.
     */
    class TwitterStreamSingleTupleExtractorImpl implements StreamSingleTupleExtractor<String, Long, String> {
        @Override public Map.Entry<Long, String> extract(String tweet) {
            try {
                Status status = TwitterObjectFactory.createStatus(tweet);

                return new IgniteBiTuple<>(status.getId(), status.getText());
            }
            catch (TwitterException e) {
                U.error(log, e);

                return null;
            }
        }
    }

}
