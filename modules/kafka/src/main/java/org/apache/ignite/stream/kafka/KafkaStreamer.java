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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.WakeupException;

/**
 * Server that subscribes to topic messages from Kafka broker and streams its to key-value pairs into
 * {@link IgniteDataStreamer} instance.
 * <p>
 * Uses Kafka's High Level Consumer API to read messages from Kafka.
 */
public class KafkaStreamer<K, V> extends StreamAdapter<ConsumerRecord, K, V> {
    /** Default polling timeout. */
    private static final long DFLT_TIMEOUT = 100;

    /** Logger. */
    private IgniteLogger log;

    /** Polling tasks executor. */
    private ExecutorService executor;

    /** Topics. */
    private List<String> topics;

    /** Number of threads. */
    private int threads;

    /** Kafka consumer config. */
    private Properties consumerCfg;

    /** Polling timeout. */
    private long timeout = DFLT_TIMEOUT;

    /** Kafka consumer tasks. */
    private final List<ConsumerTask> consumerTasks = new ArrayList<>();

    /**
     * Sets the topic names.
     *
     * @param topics Topic names.
     */
    public void setTopic(List<String> topics) {
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
     * Sets the consumer config.
     *
     * @param consumerCfg Consumer configuration.
     */
    public void setConsumerConfig(Properties consumerCfg) {
        this.consumerCfg = consumerCfg;
    }

    /**
     * Sets the polling timeout for Kafka tasks.
     *
     * @param timeout Timeout.
     */
    public void setTimeout(long timeout) {
        A.ensure(timeout > 0, "timeout > 0");

        this.timeout = timeout;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.notNull(topics, "topics");
        A.notNull(consumerCfg, "kafka consumer config");
        A.ensure(threads > 0, "threads > 0");
        A.ensure(null != getSingleTupleExtractor() || null != getMultipleTupleExtractor(),
            "Extractor must be configured");

        log = getIgnite().log();

        executor = Executors.newFixedThreadPool(threads);

        IntStream.range(0, threads).forEach(i -> consumerTasks.add(new ConsumerTask(consumerCfg)));

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
        /** Kafka consumer. */
        private final KafkaConsumer<?, ?> consumer;

        /** Stopped. */
        private volatile boolean stopped;

        /** Constructor. */
        public ConsumerTask(Properties consumerCfg) {
            this.consumer = new KafkaConsumer<>(consumerCfg);
        }

        /** {@inheritDoc} */
        @Override public Void call() {
            consumer.subscribe(topics);

            try {
                while (!stopped) {
                    for (ConsumerRecord record : consumer.poll(timeout)) {
                        try {
                            addMessage(record);
                        }
                        catch (Exception e) {
                            U.error(log, "Record is ignored due to an error [record = " + record + ']', e);
                        }
                    }
                }
            }
            catch (WakeupException we) {
                if (log.isInfoEnabled())
                    log.info("Consumer is being stopped.");
            }
            catch (KafkaException ke) {
                log.error("Kafka error", ke);
            }
            finally {
                consumer.close();
            }

            return null;
        }

        /** Stops the polling task. */
        public void stop() {
            stopped = true;

            if (consumer != null)
                consumer.wakeup();
        }
    }
}
