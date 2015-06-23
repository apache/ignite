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

package org.apache.ignite.stream.kafka;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.stream.*;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.*;
import kafka.serializer.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Server that subscribes to topic messages from Kafka broker, streams its to key-value pairs into {@link
 * org.apache.ignite.IgniteDataStreamer} instance.
 * <p>
 * Uses Kafka's High Level Consumer API to read messages from Kafka
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example">Consumer Consumer Group
 * Example</a>
 */
public class KafkaStreamer<T, K, V>
    extends StreamAdapter<T, K, V> {

    /** Logger. */
    private IgniteLogger log;

    /** Executor used to submit kafka streams. */
    private ExecutorService executor;

    /** Topic. */
    private String topic;

    /** Number of threads to process kafka streams. */
    private int threads;

    /** Kafka Consumer Config. */
    private ConsumerConfig consumerConfig;

    /** Key Decoder. */
    private Decoder<K> keyDecoder;

    /** Value Decoder. */
    private Decoder<V> valueDecoder;

    /** Kafka Consumer connector. */
    private ConsumerConnector consumer;

    /**
     * Sets the topic.
     *
     * @param topic Topic Name.
     */
    public void setTopic(final String topic) {
        this.topic = topic;
    }

    /**
     * Sets the threads.
     *
     * @param threads Number of Threads.
     */
    public void setThreads(final int threads) {
        this.threads = threads;
    }

    /**
     * Sets the consumer config.
     *
     * @param consumerConfig  Consumer configuration.
     */
    public void setConsumerConfig(final ConsumerConfig consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    /**
     * Sets the key decoder.
     *
     * @param keyDecoder Key Decoder.
     */
    public void setKeyDecoder(final Decoder<K> keyDecoder) {
        this.keyDecoder = keyDecoder;
    }

    /**
     * Sets the value decoder.
     *
     * @param valueDecoder Value Decoder
     */
    public void setValueDecoder(final Decoder<V> valueDecoder) {
        this.valueDecoder = valueDecoder;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.notNull(topic, "topic");
        A.notNull(keyDecoder, "key decoder");
        A.notNull(valueDecoder, "value decoder");
        A.notNull(consumerConfig, "kafka consumer config");
        A.ensure(threads > 0, "threads > 0");

        log = getIgnite().log();

        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, threads);

        Map<String, List<KafkaStream<K, V>>> consumerMap = consumer.createMessageStreams(topicCountMap, keyDecoder,
            valueDecoder);

        List<KafkaStream<K, V>> streams = consumerMap.get(topic);

        // Now launch all the consumer threads.
        executor = Executors.newFixedThreadPool(threads);

        // Now create an object to consume the messages.
        for (final KafkaStream<K,V> stream : streams) {
            executor.submit(new Runnable() {
                @Override public void run() {

                    ConsumerIterator<K, V> it = stream.iterator();

                    while (it.hasNext()) {
                        final MessageAndMetadata<K, V> messageAndMetadata = it.next();
                        getStreamer().addData(messageAndMetadata.key(), messageAndMetadata.message());
                    }
                }
            });
        }
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        if (consumer != null)
            consumer.shutdown();

        if (executor != null) {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
                    if (log.isDebugEnabled())
                        log.debug("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
            catch (InterruptedException e) {
                if (log.isDebugEnabled())
                    log.debug("Interrupted during shutdown, exiting uncleanly");
            }
        }
    }
}