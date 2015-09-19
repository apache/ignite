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

package org.apache.ignite.stream.storm;

import org.apache.ignite.*;
import org.apache.ignite.stream.*;
import java.util.concurrent.*;

/**
 * Server that subscribes to topic messages from Kafka broker and streams its to key-value pairs into
 * {@link IgniteDataStreamer} instance.
 * <p>
 * Uses Kafka's High Level Consumer API to read messages from Kafka.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example">Consumer Consumer Group
 * Example</a>
 */
public class StormStreamer<T, K, V> extends StreamAdapter<T, K, V> {
    /** Retry timeout. */
    private static final long DFLT_RETRY_TIMEOUT = 10000;

    /** Logger. */
    private IgniteLogger log;

    /** Executor used to submit kafka streams. */
    private ExecutorService executor;

    /** Topic. */
    private String topic;

    /** Number of threads to process kafka streams. */
    private int threads;

    /** Retry timeout. */
    private long retryTimeout = DFLT_RETRY_TIMEOUT;

    /** Stopped. */
    private volatile boolean stopped;


    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
    }

    /**
     * Stops streamer.
     */
    public void stop() {

    }
}