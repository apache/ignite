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

package org.apache.ignite.stream.pubsub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamSingleTupleExtractor;

/**
 * Server that subscribes to topic messages from Pub/Sub and streams its to key-value pairs into
 * {@link IgniteDataStreamer} instance, using Google Cloud Pub/Sub sdk as a Pub/Sub client.
 * <p>
 * You must also provide a {@link StreamSingleTupleExtractor} or a {@link StreamMultipleTupleExtractor} to extract
 * cache tuples out of the incoming message.
 * <p>
 */
public class PubSubStreamer<K, V> extends StreamAdapter<PubsubMessage, K, V> {
    /** Default max messages. */
    private static final int DFLT_MAX_MESSAGES = 10;

    /** Logger. */
    private IgniteLogger log;

    /** Polling tasks executor. */
    private ExecutorService executor;

    /** Topics. */
    private List<ProjectTopicName> topics;

    /** Number of threads. */
    private int threads;

    /** Pub/Sub subscriptionName. */
    private String subscriptionName;

    /** Pub/Sub subscriberStubSettings*/
    private SubscriberStubSettings subscriberStubSettings;

    /** Return policy on unavailable messages. */
    private boolean returnImmediately = false;

    /** Pub/Sub maxMessages. */
    private int maxMessages = DFLT_MAX_MESSAGES;

    /** Pub/Sub consumer tasks. */
    private final List<ConsumerTask> consumerTasks = new ArrayList<>();

    /**
     * Sets the topic names.
     *
     * @param topics Topic names.
     */
    public void setTopic(List<ProjectTopicName> topics) {
        this.topics = topics;
    }

    /**
     * Sets the threads.
     *
     * @param threads Number of threads.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Sets the consumer subscriptionName.
     *
     * @param subscriptionName Consumer subscription name.
     */
    public void setSubscriptionName(String subscriptionName) {
        this.subscriptionName = subscriptionName;
    }

    /**
     * Sets the consumer Settings
     * @param subscriberStubSettings Pub/Sub subscriber settings
     */
    public void setSubscriberStubSettings(SubscriberStubSettings subscriberStubSettings) {
        this.subscriberStubSettings = subscriberStubSettings;
    }

    /**
     * Sets the return policy for receiving messages on Pub/Sub tasks on message availability.
     *
     * @param returnImmediately Pub/Sub waiting policy on message unavailability.
     */
    public void setReturnImmediately(boolean returnImmediately) {
        this.returnImmediately = returnImmediately;
    }

    /**
     * Sets the max number of Pub/Sub messages to fetch on pull request
     * @param maxMessages Pub/Sub messages per pull request
     */
    public void setMaxMessages(int maxMessages) {
        this.maxMessages = maxMessages;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.ensure(!(getSingleTupleExtractor() == null && getMultipleTupleExtractor() == null),
                 "tuple extractor missing");
        A.ensure(getSingleTupleExtractor() == null || getMultipleTupleExtractor() == null,
                 "cannot provide both single and multiple tuple extractor");
        A.notNull(topics, "topics");
        A.notNull(subscriptionName, "Pub/Sub consumer config");
        A.ensure(threads > 0, "threads > 0");

        log = getIgnite().log();

        executor = Executors.newFixedThreadPool(threads);

        IntStream.range(0, threads).forEach(i -> consumerTasks.add(new ConsumerTask(subscriberStubSettings, subscriptionName,
            returnImmediately, maxMessages)));

        for (ConsumerTask task : consumerTasks)
            executor.submit(task);
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        for (ConsumerTask task : consumerTasks)
            task.stop();

        if (executor != null) {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
                    if (log.isDebugEnabled())
                        log.debug("Timed out waiting for consumer threads to shut down, exiting uncleanly.");
            }
            catch (InterruptedException ignored) {
                if (log.isDebugEnabled())
                    log.debug("Interrupted during shutdown, exiting uncleanly.");
            }
        }
    }

    /** Polling task. */
    class ConsumerTask implements Callable<Void> {
        /** Pub/Sub consumer */
        private final SubscriberStub subscriberStub;

        /** */
        private final String subscriptionName;

        /** */
        private final boolean returnImmediately;

        /** */
        private final int maxMessages;

        /** Stopped. */
        private volatile boolean stopped;

        /**
         * @param subscriberStubSettings
         * @param subscriptionName
         * @param returnImmediately
         * @param maxMessages
         * @throws IgniteException
         */
        public ConsumerTask(
            SubscriberStubSettings subscriberStubSettings,
            String subscriptionName,
            boolean returnImmediately,
            int maxMessages
        ) throws IgniteException {
            try {
                this.subscriberStub = GrpcSubscriberStub.create(subscriberStubSettings);
                this.subscriptionName = subscriptionName;
                this.returnImmediately = returnImmediately;
                this.maxMessages = maxMessages;
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            try {
                while (!stopped) {
                    PullRequest pullRequest =
                            PullRequest.newBuilder()
                                       .setMaxMessages(maxMessages)
                                       .setReturnImmediately(returnImmediately) // return immediately if messages are not available
                                       .setSubscription(subscriptionName)
                                       .build();

                    PullResponse pullResponse = subscriberStub.pullCallable().call(pullRequest);

                    List<String> ackIds = new ArrayList<>();
                    for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
                        addMessage(message.getMessage());
                        ackIds.add(message.getAckId());
                    }

                    AcknowledgeRequest acknowledgeRequest = AcknowledgeRequest.newBuilder()
                                                                              .setSubscription(subscriptionName)
                                                                              .addAllAckIds(ackIds)
                                                                              .build();

                    subscriberStub.acknowledgeCallable().call(acknowledgeRequest);
                }
            }
            finally {
                subscriberStub.close();
            }

            return null;
        }

        /** Stops the polling task. */
        public void stop() {
            stopped = true;

            if (subscriberStub != null) {
                subscriberStub.shutdown();
            }
        }
    }

}
