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
import org.eclipse.paho.client.mqttv3.MqttException;
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

    private volatile boolean stopped = true;

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
            A.ensure(getSingleTupleExtractor() == null && getMultipleTupleExtractor() == null, "tuple extractor missing");
            A.ensure(getSingleTupleExtractor() == null || getMultipleTupleExtractor() == null, "cannot provide " +
                "both single and multiple tuple extractor");
            A.notNullOrEmpty(brokerUrl, "broker URL");
            A.notNullOrEmpty(clientId, "client ID");

            // if we have both a single topic and a list of topics, fail
            if (topic != null && topic.length() > 0 && !topics.isEmpty())
                throw new IllegalArgumentException("Cannot specify both a single topic and a list at the same time");

            // if we have both a single QoS and list, fail
            if (qualityOfService != null && !qualitiesOfService.isEmpty()) {
                throw new IllegalArgumentException("Cannot specify both a single QoS and a list at the same time");
            }

            // Paho API requires disconnect timeout if providing a quiesce timeout and disconnecting forcibly
            if (disconnectForcibly && disconnectQuiesceTimeout != null) {
                A.notNull(disconnectForciblyTimeout, "disconnect timeout cannot be null when disconnecting forcibly " +
                    "with quiesce");
            }

            // if we have multiple topics
            if (topics != null && !topics.isEmpty()) {
                for (String t : topics) {
                    A.notNullOrEmpty(t, "topic in list of topics");
                }
                A.ensure(qualitiesOfService.isEmpty() || qualitiesOfService.size() == topics.size(), "qualities of " +
                    "service must be either empty or have the same size as topics list");
            }

            // create logger
            log = getIgnite().log();

            // create the mqtt client
            if (persistence == null)
                client = new MqttClient(brokerUrl, clientId);
            else
                client = new MqttClient(brokerUrl, clientId, persistence);

            connectAndSubscribe();

            stopped = false;

        }
        catch (Throwable t) {
            throw new IgniteException("Exception while initializing MqttStreamer", t);
        }

    }

    private void connectAndSubscribe() throws MqttException {
        // connect
        if (connectOptions != null)
            client.connect();
        else
            client.connect(connectOptions);

        // subscribe to multiple topics
        if (!topics.isEmpty()) {
            if (qualitiesOfService.isEmpty()) {
                client.subscribe(topics.toArray(new String[0]));
            } else {
                int[] qoses = new int[qualitiesOfService.size()];
                for (int i = 0; i < qualitiesOfService.size(); i++)
                    qoses[i] = qualitiesOfService.get(i);

                client.subscribe(topics.toArray(new String[0]), qoses);
            }
        } else {
            // subscribe to a single topic
            if (qualityOfService == null) {
                client.subscribe(topic);
            } else {
                client.subscribe(topic, qualityOfService);
            }
        }
    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {
        if (stopped)
            throw new IgniteException("Attempted to stop an already stopped MQTT Streamer");

        try {
            if (disconnectForcibly) {
                if (disconnectQuiesceTimeout == null && disconnectForciblyTimeout == null) {
                    client.disconnectForcibly();
                } else if (disconnectForciblyTimeout != null && disconnectQuiesceTimeout == null) {
                    client.disconnectForcibly(disconnectForciblyTimeout);
                } else {
                    client.disconnectForcibly(disconnectQuiesceTimeout, disconnectForciblyTimeout);
                }
            } else {
                if (disconnectQuiesceTimeout == null) {
                    client.disconnect();
                } else {
                    client.disconnect(disconnectQuiesceTimeout);
                }
            }
        }
        catch (Throwable t) {
            throw new IgniteException("Exception while stopping MqttStreamer", t);
        }
    }

    @Override public void connectionLost(Throwable throwable) {
        log.warning(String.format("MQTT Connection to server %s was lost due to", brokerUrl), throwable);
        // TODO: handle reconnect attempts with an optional backoff mechanism (linear, exponential, finonacci)
        try {
            connectAndSubscribe();
        }
        catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override public void messageArrived(String topic, MqttMessage message) throws Exception {
        if (getMultipleTupleExtractor() != null) {
            Map<K, V> entries = getMultipleTupleExtractor().extract(message);
            getStreamer().addData(entries);
        } else {
            Map.Entry<K, V> entry = getSingleTupleExtractor().extract(message);
            getStreamer().addData(entry);
        }
    }

    @Override public void deliveryComplete(IMqttDeliveryToken token) {
        // ignore, we don't send messages
    }
}