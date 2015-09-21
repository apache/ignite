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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.apache.ignite.stream.StreamSingleTupleExtractor;

import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.StopStrategy;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
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
 *     <li>Specifying the client ID.</li>
 * </ul>
 *
 * Note: features like durable subscriptions, last will testament, etc. must be configured via the
 * {@link #connectOptions} property.
 *
 * @author Raul Kripalani
 */
public class MqttStreamer<K, V> extends StreamAdapter<MqttMessage, K, V> implements MqttCallback {

    /** Logger. */
    private IgniteLogger log;

    private MqttClient client;

    private String brokerUrl;

    private String topic;

    private Integer qualityOfService;

    private List<String> topics;

    private List<Integer> qualitiesOfService;

    /** Client ID in case we're using durable subscribers. */
    private String clientId;

    private MqttClientPersistence persistence;

    private MqttConnectOptions connectOptions;

    // disconnect parameters
    private Integer disconnectQuiesceTimeout;

    private boolean disconnectForcibly;

    private Integer disconnectForciblyTimeout;

    private WaitStrategy retryWaitStrategy = WaitStrategies.fibonacciWait();

    private StopStrategy retryStopStrategy = StopStrategies.neverStop();

    private MqttConnectionRetrier connectionRetrier;

    private volatile boolean stopped = true;

    private volatile boolean connected;

    private String cachedLogPrefix;

    private boolean blockUntilConnected;

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IgniteException {
        if (!stopped)
            throw new IgniteException("Attempted to start an already started MQTT Streamer");

        // for simplicity, if these are null initialize to empty lists
        topics = topics == null ? new ArrayList<String>() : topics;
        qualitiesOfService = qualitiesOfService == null ? new ArrayList<Integer>() : qualitiesOfService;

        try {
            // parameter validations
            A.notNull(getStreamer(), "streamer");
            A.notNull(getIgnite(), "ignite");
            A.ensure(!(getSingleTupleExtractor() == null && getMultipleTupleExtractor() == null), "tuple extractor missing");
            A.ensure(getSingleTupleExtractor() == null || getMultipleTupleExtractor() == null, "cannot provide " +
                "both single and multiple tuple extractor");
            A.notNullOrEmpty(brokerUrl, "broker URL");
            A.notNullOrEmpty(clientId, "client ID");

            // if we have both a single topic and a list of topics (but the list of topic is not of
            // size 1 and == topic, as this would be a case of re-initialization), fail
            if (topic != null && topic.length() > 0 && !topics.isEmpty() &&
                topics.size() != 1 && !topics.get(0).equals(topic))
                throw new IllegalArgumentException("Cannot specify both a single topic and a list at the same time");

            // same as above but for QoS
            if (qualityOfService != null && !qualitiesOfService.isEmpty() && qualitiesOfService.size() != 1 &&
                !qualitiesOfService.get(0).equals(qualityOfService)) {
                throw new IllegalArgumentException("Cannot specify both a single QoS and a list at the same time");
            }

            // Paho API requires disconnect timeout if providing a quiesce timeout and disconnecting forcibly
            if (disconnectForcibly && disconnectQuiesceTimeout != null) {
                A.notNull(disconnectForciblyTimeout, "disconnect timeout cannot be null when disconnecting forcibly " +
                    "with quiesce");
            }

            // if we have multiple topics
            if (!topics.isEmpty()) {
                for (String t : topics)
                    A.notNullOrEmpty(t, "topic in list of topics");

                A.ensure(qualitiesOfService.isEmpty() || qualitiesOfService.size() == topics.size(), "qualities of " +
                    "service must be either empty or have the same size as topics list");

                cachedLogPrefix = "[" + Joiner.on(",").join(topics) + "]";
            }
            else {  // just the single topic
                topics.add(topic);

                if (qualityOfService != null)
                    qualitiesOfService.add(qualityOfService);

                cachedLogPrefix = "[" + topic + "]";
            }

            // create logger
            log = getIgnite().log();

            // create the mqtt client
            if (persistence == null)
                client = new MqttClient(brokerUrl, clientId);
            else
                client = new MqttClient(brokerUrl, clientId, persistence);

            // set this as a callback
            client.setCallback(this);

            // set stopped to false, as the connection will start async
            stopped = false;

            // build retrier
            Retryer<Boolean> retrier = RetryerBuilder.<Boolean>newBuilder()
                .retryIfResult(new Predicate<Boolean>() {
                    @Override public boolean apply(Boolean connected) {
                        return !connected;
                    }
                })
                .retryIfException().retryIfRuntimeException()
                .withWaitStrategy(retryWaitStrategy)
                .withStopStrategy(retryStopStrategy)
                .build();

            // create the connection retrier
            connectionRetrier = new MqttConnectionRetrier(retrier);
            connectionRetrier.connect();

        }
        catch (Throwable t) {
            throw new IgniteException("Exception while initializing MqttStreamer", t);
        }

    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {
        if (stopped)
            throw new IgniteException("Attempted to stop an already stopped MQTT Streamer");

        // stop the retrier
        connectionRetrier.stop();

        try {
            if (disconnectForcibly) {
                if (disconnectQuiesceTimeout == null && disconnectForciblyTimeout == null)
                    client.disconnectForcibly();

                else if (disconnectForciblyTimeout != null && disconnectQuiesceTimeout == null)
                    client.disconnectForcibly(disconnectForciblyTimeout);

                else
                    client.disconnectForcibly(disconnectQuiesceTimeout, disconnectForciblyTimeout);

            } else {
                if (disconnectQuiesceTimeout == null)
                    client.disconnect();

                else
                    client.disconnect(disconnectQuiesceTimeout);

            }

            client.close();
            connected = false;
            stopped = true;

        }
        catch (Throwable t) {
            throw new IgniteException("Exception while stopping MqttStreamer", t);
        }
    }

    // -------------------------------
    //  MQTT Client callback methods
    // -------------------------------

    @Override public void connectionLost(Throwable throwable) {
        connected = false;

        // if we have been stopped, we do not try to establish the connection again
        if (stopped)
            return;

        log.warning(String.format("MQTT Connection to server %s was lost.", brokerUrl), throwable);
        connectionRetrier.connect();
    }

    @Override public void messageArrived(String topic, MqttMessage message) throws Exception {
        if (getMultipleTupleExtractor() != null) {
            Map<K, V> entries = getMultipleTupleExtractor().extract(message);
            if (log.isTraceEnabled()) {
                log.trace("Adding cache entries: " + entries);
            }
            getStreamer().addData(entries);
        }
        else {
            Map.Entry<K, V> entry = getSingleTupleExtractor().extract(message);
            if (log.isTraceEnabled()) {
                log.trace("Adding cache entry: " + entry);
            }
            getStreamer().addData(entry);
        }
    }

    @Override public void deliveryComplete(IMqttDeliveryToken token) {
        // ignore, as we don't send messages
    }

    // -------------------------------
    //  Getters and setters
    // -------------------------------

    public String getBrokerUrl() {
        return brokerUrl;
    }

    public void setBrokerUrl(String brokerUrl) {
        this.brokerUrl = brokerUrl;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getQualityOfService() {
        return qualityOfService;
    }

    public void setQualityOfService(Integer qualityOfService) {
        this.qualityOfService = qualityOfService;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<Integer> getQualitiesOfService() {
        return qualitiesOfService;
    }

    public void setQualitiesOfService(List<Integer> qualitiesOfService) {
        this.qualitiesOfService = qualitiesOfService;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public MqttClientPersistence getPersistence() {
        return persistence;
    }

    public void setPersistence(MqttClientPersistence persistence) {
        this.persistence = persistence;
    }

    public MqttConnectOptions getConnectOptions() {
        return connectOptions;
    }

    public void setConnectOptions(MqttConnectOptions connectOptions) {
        this.connectOptions = connectOptions;
    }

    public boolean isDisconnectForcibly() {
        return disconnectForcibly;
    }

    public void setDisconnectForcibly(boolean disconnectForcibly) {
        this.disconnectForcibly = disconnectForcibly;
    }

    public Integer getDisconnectQuiesceTimeout() {
        return disconnectQuiesceTimeout;
    }

    public void setDisconnectQuiesceTimeout(Integer disconnectQuiesceTimeout) {
        this.disconnectQuiesceTimeout = disconnectQuiesceTimeout;
    }

    public Integer getDisconnectForciblyTimeout() {
        return disconnectForciblyTimeout;
    }

    public void setDisconnectForciblyTimeout(Integer disconnectForciblyTimeout) {
        this.disconnectForciblyTimeout = disconnectForciblyTimeout;
    }

    public WaitStrategy getRetryWaitStrategy() {
        return retryWaitStrategy;
    }

    public void setRetryWaitStrategy(WaitStrategy retryWaitStrategy) {
        this.retryWaitStrategy = retryWaitStrategy;
    }

    public StopStrategy getRetryStopStrategy() {
        return retryStopStrategy;
    }

    public void setRetryStopStrategy(StopStrategy retryStopStrategy) {
        this.retryStopStrategy = retryStopStrategy;
    }

    public boolean isBlockUntilConnected() {
        return blockUntilConnected;
    }

    public void setBlockUntilConnected(boolean blockUntilConnected) {
        this.blockUntilConnected = blockUntilConnected;
    }

    private class MqttConnectionRetrier {

        private final Retryer<Boolean> retrier;
        private ExecutorService executor = Executors.newSingleThreadExecutor();

        public MqttConnectionRetrier(Retryer<Boolean> retrier) {
            this.retrier = retrier;
        }

        public void connect() {
            Callable<Boolean> callable = retrier.wrap(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    // if we're already connected, return immediately
                    if (connected)
                        return true;

                    if (stopped)
                        return false;

                    // connect to broker
                    if (connectOptions == null)
                        client.connect();
                    else
                        client.connect(connectOptions);

                    // always use the multiple topics variant of the mqtt client; even if the user specified a single
                    // topic and/or QoS, the initialization code would have placed it inside the 1..n structures
                    if (qualitiesOfService.isEmpty())
                        client.subscribe(topics.toArray(new String[0]));

                    else {
                        int[] qoses = new int[qualitiesOfService.size()];
                        for (int i = 0; i < qualitiesOfService.size(); i++)
                            qoses[i] = qualitiesOfService.get(i);

                        client.subscribe(topics.toArray(new String[0]), qoses);
                    }

                    connected = true;
                    return connected;
                }
            });

            Future<Boolean> result = executor.submit(callable);

            if (blockUntilConnected) {
                try {
                    result.get();
                }
                catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void stop() {
            executor.shutdownNow();
        }

    }

}