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

package org.apache.ignite.stream.mqtt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.StopStrategy;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Streamer that consumes from a MQTT topic and feeds key-value pairs into an {@link IgniteDataStreamer} instance,
 * using Eclipse Paho as an MQTT client.
 * <p>
 * You must also provide a {@link StreamSingleTupleExtractor} or a {@link StreamMultipleTupleExtractor} to extract
 * cache tuples out of the incoming message.
 * <p>
 * This Streamer has many features:
 *
 * <ul>
 *     <li>Subscribing to a single topic or multiple topics at once.</li>
 *     <li>Specifying the subscriber's QoS for a single topic or for multiple topics.</li>
 *     <li>Allows setting {@link MqttConnectOptions} to support features like last will testament, persistent
 *         sessions, etc.</li>
 *     <li>Specifying the client ID. A random one will be generated and maintained throughout reconnections if the user
 *         does not provide one.</li>
 *     <li>(Re-)Connection retries based on the <i>guava-retrying</i> library. Retry wait and retry stop policies
 *         can be configured.</li>
 *     <li>Blocking the start() method until connected for the first time.</li>
 * </ul>
 *
 * Note: features like durable subscriptions, last will testament, etc. can be configured via the
 * {@link #setConnectOptions(MqttConnectOptions)} setter.
 *
 * @see <a href="https://github.com/rholder/guava-retrying">guava-retrying library</a>
 */
public class MqttStreamer<K, V> extends StreamAdapter<MqttMessage, K, V> implements MqttCallback {
    /** Logger. */
    private IgniteLogger log;

    /** The MQTT client object for internal use. */
    private MqttClient client;

    /** The broker URL, set by the user. */
    private String brokerUrl;

    /** The topic to subscribe to, if a single topic. */
    private String topic;

    /** The quality of service to use for a single topic subscription (optional). */
    private Integer qualityOfService;

    /** The topics to subscribe to, if many. */
    private List<String> topics;

    /**
     * The qualities of service to use for multiple topic subscriptions. If specified, it must contain the same
     * number of elements as {@link #topics}.
     */
    private List<Integer> qualitiesOfService;

    /** The MQTT client ID (optional). */
    private String clientId;

    /** A configurable persistence mechanism. If not set, Paho will use its default. */
    private MqttClientPersistence persistence;

    /** The MQTT client connect options, where users can configured the last will and testament, durability, etc. */
    private MqttConnectOptions connectOptions;

    /** Quiesce timeout on disconnection. */
    private Integer disconnectQuiesceTimeout;

    /** Whether to disconnect forcibly or not. */
    private boolean disconnectForcibly;

    /** If disconnecting forcibly, the timeout. */
    private Integer disconnectForciblyTimeout;

    /**
     * The strategy to determine how long to wait between retry attempts. By default, this streamer uses a
     * Fibonacci-based strategy.
     */
    private WaitStrategy retryWaitStrategy = WaitStrategies.fibonacciWait();

    /** The strategy to determine when to stop retrying to (re-)connect. By default, we never stop. */
    private StopStrategy retryStopStrategy = StopStrategies.neverStop();

    /** The internal connection retrier object with a thread pool of size 1. */
    private MqttConnectionRetrier connectionRetrier;

    /** Whether to block the start() method until connected for the first time. */
    private boolean blockUntilConnected;

    /** State keeping. */
    private volatile boolean stopped = true;

    /** Cached log prefix for cache messages. */
    private String cachedLogValues;

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IgniteException {
        if (!stopped)
            throw new IgniteException("Attempted to start an already started MQTT Streamer");

        // For simplicity, if these are null initialize to empty lists.
        topics = topics == null ? new ArrayList<String>() : topics;

        qualitiesOfService = qualitiesOfService == null ? new ArrayList<Integer>() : qualitiesOfService;

        try {
            Map<String, Object> logValues = new HashMap<>();

            // Parameter validations.
            A.notNull(getStreamer(), "streamer");
            A.notNull(getIgnite(), "ignite");
            A.ensure(!(getSingleTupleExtractor() == null && getMultipleTupleExtractor() == null),
                "tuple extractor missing");
            A.ensure(getSingleTupleExtractor() == null || getMultipleTupleExtractor() == null,
                "cannot provide both single and multiple tuple extractor");
            A.notNullOrEmpty(brokerUrl, "broker URL");

            // If the client ID is empty, generate one.
            if (clientId == null || clientId.length() == 0)
                clientId = MqttClient.generateClientId();

            // If we have both a single topic and a list of topics (but the list of topic is not of
            // size 1 and == topic, as this would be a case of re-initialization), fail.
            if (topic != null && topic.length() > 0 && !topics.isEmpty() &&
                topics.size() != 1 && !topics.get(0).equals(topic))
                throw new IllegalArgumentException("Cannot specify both a single topic and a list at the same time.");

            // Same as above but for QoS.
            if (qualityOfService != null && !qualitiesOfService.isEmpty() && qualitiesOfService.size() != 1 &&
                !qualitiesOfService.get(0).equals(qualityOfService))
                throw new IllegalArgumentException("Cannot specify both a single QoS and a list at the same time.");

            // Paho API requires disconnect timeout if providing a quiesce timeout and disconnecting forcibly.
            if (disconnectForcibly && disconnectQuiesceTimeout != null)
                A.notNull(disconnectForciblyTimeout, "disconnect timeout cannot be null when disconnecting forcibly " +
                    "with quiesce");

            // If we have multiple topics.
            if (!topics.isEmpty()) {
                for (String t : topics)
                    A.notNullOrEmpty(t, "topic in list of topics");

                A.ensure(qualitiesOfService.isEmpty() || qualitiesOfService.size() == topics.size(),
                    "qualities of service must be either empty or have the same size as topics list");

                logValues.put("topics", topics);
            }
            else {
                // Just the single topic.
                topics.add(topic);

                if (qualityOfService != null)
                    qualitiesOfService.add(qualityOfService);

                logValues.put("topic", topic);
            }

            // Finish building log values.
            logValues.put("brokerUrl", brokerUrl);
            logValues.put("clientId", clientId);

            // Cache log values.
            cachedLogValues = "[" + Joiner.on(", ").withKeyValueSeparator("=").join(logValues) + "]";

            // Create logger.
            log = getIgnite().log();

            // Create the MQTT client.
            if (persistence == null)
                client = new MqttClient(brokerUrl, clientId);
            else
                client = new MqttClient(brokerUrl, clientId, persistence);

            // Set this as a callback.
            client.setCallback(this);

            // Set stopped to false, as the connection will start async.
            stopped = false;

            // Build retrier.
            Retryer<Void> retrier = RetryerBuilder.<Void>newBuilder()
                .retryIfResult(new Predicate<Void>() {
                    @Override public boolean apply(Void v) {
                        return !client.isConnected() && !stopped;
                    }
                })
                .retryIfException().retryIfRuntimeException()
                .withWaitStrategy(retryWaitStrategy)
                .withStopStrategy(retryStopStrategy)
                .build();

            // Create the connection retrier.
            connectionRetrier = new MqttConnectionRetrier(retrier);

            if (log.isInfoEnabled())
                log.info("Starting MQTT Streamer " + cachedLogValues);

            // Connect.
            connectionRetrier.connect();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to initialize MQTT Streamer.", e);
        }
    }

    /**
     * Stops streamer.
     *
     * @throws IgniteException If failed.
     */
    public void stop() throws IgniteException {
        if (stopped)
            throw new IgniteException("Failed to stop MQTT Streamer (already stopped).");

        // Stop the retrier.
        connectionRetrier.stop();

        try {
            if (disconnectForcibly) {
                if (disconnectQuiesceTimeout == null && disconnectForciblyTimeout == null)
                    client.disconnectForcibly();

                else if (disconnectForciblyTimeout != null && disconnectQuiesceTimeout == null)
                    client.disconnectForcibly(disconnectForciblyTimeout);

                else
                    client.disconnectForcibly(disconnectQuiesceTimeout, disconnectForciblyTimeout);
            }
            else {
                if (disconnectQuiesceTimeout == null)
                    client.disconnect();

                else
                    client.disconnect(disconnectQuiesceTimeout);
            }

            client.close();

            stopped = true;
        }
        catch (Exception e) {
            throw new IgniteException("Failed to stop Exception while stopping MQTT Streamer.", e);
        }
    }

    // -------------------------------
    //  MQTT Client callback methods
    // -------------------------------

    /**
     * Implements the {@link MqttCallback#connectionLost(Throwable)} callback method for the MQTT client to inform the
     * streamer that the connection has been lost.
     *
     * {@inheritDoc}
     */
    @Override public void connectionLost(Throwable throwable) {
        // If we have been stopped, we do not try to establish the connection again.
        if (stopped)
            return;

        log.warning(String.format("MQTT Connection to broker was lost [brokerUrl=%s, type=%s, err=%s]", brokerUrl,
            throwable.getClass(), throwable.getMessage()));

        connectionRetrier.connect();
    }

    /**
     * Implements the {@link MqttCallback#messageArrived(String, MqttMessage)} to receive an MQTT message.
     *
     * {@inheritDoc}
     */
    @Override public void messageArrived(String topic, MqttMessage message) throws Exception {
        if (getMultipleTupleExtractor() != null) {
            Map<K, V> entries = getMultipleTupleExtractor().extract(message);

            if (log.isTraceEnabled())
                log.trace("Adding cache entries: " + entries);

            getStreamer().addData(entries);
        }
        else {
            Map.Entry<K, V> entry = getSingleTupleExtractor().extract(message);

            if (log.isTraceEnabled())
                log.trace("Adding cache entry: " + entry);

            getStreamer().addData(entry);
        }
    }

    /**
     * Empty implementation of {@link MqttCallback#deliveryComplete(IMqttDeliveryToken)}.
     *
     * Not required by the streamer as it doesn't produce messages.
     *
     * {@inheritDoc}
     */
    @Override public void deliveryComplete(IMqttDeliveryToken token) {
        // ignore, as we don't send messages
    }

    // -------------------------------
    //  Getters and setters
    // -------------------------------

    /**
     * Sets the broker URL (compulsory).
     *
     * @param brokerUrl The Broker URL (compulsory).
     */
    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    /**
     * Gets the broker URL.
     *
     * @return The Broker URL.
     */
    public String getBrokerUrl() {
        return brokerUrl;
    }

    /**
     * Sets the topic to subscribe to, if a single topic.
     *
     * @param topic The topic to subscribe to.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Gets the subscribed topic.
     *
     * @return The subscribed topic.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Sets the quality of service to use for a single topic subscription (optional).
     *
     * @param qualityOfService The quality of service.
     */
    public void setQualityOfService(Integer qualityOfService) {
        this.qualityOfService = qualityOfService;
    }

    /**
     * Gets the quality of service set by the user for a single topic consumption.
     *
     * @return The quality of service.
     */
    public Integer getQualityOfService() {
        return qualityOfService;
    }

    /**
     * Sets the topics to subscribe to, if many.
     *
     * @param topics The topics.
     */
    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    /**
     * Gets the topics subscribed to.
     *
     * @return The topics subscribed to.
     */
    public List<String> getTopics() {
        return topics;
    }

    /**
     * Sets the qualities of service to use for multiple topic subscriptions. If specified, the list must contain the
     * same number of elements as {@link #topics}.
     *
     * @param qualitiesOfService The qualities of service.
     */
    public void setQualitiesOfService(List<Integer> qualitiesOfService) {
        this.qualitiesOfService = qualitiesOfService;
    }

    /**
     * Gets the qualities of service for multiple topics.
     *
     * @return The qualities of service.
     */
    public List<Integer> getQualitiesOfService() {
        return qualitiesOfService;
    }

    /**
     * Sets the MQTT client ID (optional). If one is not provided, the streamer will generate one and will maintain
     * it througout any reconnection attempts.
     *
     * @param clientId The client ID.
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Gets the client ID, either the one set by the user or the automatically generated one.
     *
     * @return The client ID.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Gets the currently set persistence mechanism.
     *
     * @return The persistence mechanism.
     */
    public MqttClientPersistence getPersistence() {
        return persistence;
    }

    /**
     * Sets the persistence mechanism. If not set, Paho will use its default.
     *
     * @param persistence A configurable persistence mechanism.
     */
    public void setPersistence(MqttClientPersistence persistence) {
        this.persistence = persistence;
    }

    /**
     * Gets the currently used MQTT client connect options.
     *
     * @return The MQTT client connect options.
     */
    public MqttConnectOptions getConnectOptions() {
        return connectOptions;
    }

    /**
     * Sets the MQTT client connect options, where users can configured the last will and testament, durability, etc.
     *
     * @param connectOptions The MQTT client connect options.
     */
    public void setConnectOptions(MqttConnectOptions connectOptions) {
        this.connectOptions = connectOptions;
    }

    /**
     * Sets whether to disconnect forcibly or not when shutting down. By default, it's {@code false}.
     *
     * @param disconnectForcibly Whether to disconnect forcibly or not. By default, it's {@code false}.
     */
    public void setDisconnectForcibly(boolean disconnectForcibly) {
        this.disconnectForcibly = disconnectForcibly;
    }

    /**
     * Gets whether this MQTT client will disconnect forcibly when shutting down.
     *
     * @return Whether to disconnect forcibly or not.
     */
    public boolean isDisconnectForcibly() {
        return disconnectForcibly;
    }

    /**
     * Sets the quiesce timeout on disconnection. If not provided, this streamer won't use any.
     *
     * @param disconnectQuiesceTimeout The disconnect quiesce timeout.
     */
    public void setDisconnectQuiesceTimeout(Integer disconnectQuiesceTimeout) {
        this.disconnectQuiesceTimeout = disconnectQuiesceTimeout;
    }

    /**
     * Gets the disconnect quiesce timeout.
     *
     * @return The disconnect quiesce timeout.
     */
    public Integer getDisconnectQuiesceTimeout() {
        return disconnectQuiesceTimeout;
    }

    /**
     * Sets the timeout if disconnecting forcibly. Compulsory in that case.
     *
     * @param disconnectForciblyTimeout The disconnect forcibly timeout.
     */
    public void setDisconnectForciblyTimeout(Integer disconnectForciblyTimeout) {
        this.disconnectForciblyTimeout = disconnectForciblyTimeout;
    }

    /**
     * Gets the timeout if disconnecting forcibly.
     *
     * @return Timeout.
     */
    public Integer getDisconnectForciblyTimeout() {
        return disconnectForciblyTimeout;
    }

    /**
     * Sets the strategy to determine how long to wait between retry attempts. By default, this streamer uses a
     * Fibonacci-based strategy.
     *
     * @param retryWaitStrategy The retry wait strategy.
     */
    public void setRetryWaitStrategy(WaitStrategy retryWaitStrategy) {
        this.retryWaitStrategy = retryWaitStrategy;
    }

    /**
     * Gets the retry wait strategy.
     *
     * @return The retry wait strategy.
     */
    public WaitStrategy getRetryWaitStrategy() {
        return retryWaitStrategy;
    }

    /**
     * Sets the strategy to determine when to stop retrying to (re-)connect. By default, we never stop.
     *
     * @param retryStopStrategy The retry stop strategy.
     */
    public void setRetryStopStrategy(StopStrategy retryStopStrategy) {
        this.retryStopStrategy = retryStopStrategy;
    }

    /**
     * Gets the retry stop strategy.
     *
     * @return The retry stop strategy.
     */
    public StopStrategy getRetryStopStrategy() {
        return retryStopStrategy;
    }

    /**
     * Sets whether to block the start() method until connected for the first time. By default, it's {@code false}.
     *
     * @param blockUntilConnected Whether to block or not.
     */
    public void setBlockUntilConnected(boolean blockUntilConnected) {
        this.blockUntilConnected = blockUntilConnected;
    }

    /**
     * Gets whether to block the start() method until connected for the first time. By default, it's {@code false}.
     *
     * @return {@code true} if should connect synchronously in start.
     */
    public boolean isBlockUntilConnected() {
        return blockUntilConnected;
    }

    /**
     * Returns whether this streamer is stopped.
     *
     * @return {@code true} if stopped; {@code false} if not.
     */
    public boolean isStopped() {
        return stopped;
    }

    /**
     * Returns whether this streamer is connected by delegating to the underlying {@link MqttClient#isConnected()}
     *
     * @return {@code true} if connected; {@code false} if not.
     * @see MqttClient#isConnected()
     */
    public boolean isConnected() {
        return client.isConnected();
    }

    /**
     * A utility class to help us with (re-)connecting to the MQTT broker. It uses a single-threaded executor to perform
     * the (re-)connections.
     */
    private class MqttConnectionRetrier {
        /** The guava-retrying retrier object. */
        private final Retryer<Void> retrier;

        /** Single-threaded pool. */
        private final ExecutorService exec = Executors.newSingleThreadExecutor();

        /**
         * Constructor.
         *
         * @param retrier The retryier object.
         */
        public MqttConnectionRetrier(Retryer<Void> retrier) {
            this.retrier = retrier;
        }

        /**
         * Method called by the streamer to ask us to (re-)connect.
         */
        public void connect() {
            Callable<Void> callable = retrier.wrap(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    // If we're already connected, return immediately.
                    if (client.isConnected())
                        return null;

                    if (stopped)
                        return null;

                    // Connect to broker.
                    if (connectOptions == null)
                        client.connect();
                    else
                        client.connect(connectOptions);

                    // Always use the multiple topics variant of the mqtt client; even if the user specified a single
                    // topic and/or QoS, the initialization code would have placed it inside the 1..n structures.
                    if (qualitiesOfService.isEmpty())
                        client.subscribe(topics.toArray(new String[0]));

                    else {
                        int[] qoses = new int[qualitiesOfService.size()];

                        for (int i = 0; i < qualitiesOfService.size(); i++)
                            qoses[i] = qualitiesOfService.get(i);

                        client.subscribe(topics.toArray(new String[0]), qoses);
                    }

                    if (log.isInfoEnabled())
                        log.info("MQTT Streamer (re-)connected and subscribed " + cachedLogValues);

                    return null;
                }
            });

            Future<Void> result = exec.submit(callable);

            if (blockUntilConnected) {
                try {
                    result.get();
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * Stops this connection utility class by shutting down the thread pool.
         */
        public void stop() {
            exec.shutdownNow();
        }
    }
}
