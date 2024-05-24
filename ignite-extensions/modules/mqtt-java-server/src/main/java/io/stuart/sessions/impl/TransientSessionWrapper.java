/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.sessions.impl;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.consts.MetricsConst;
import io.stuart.consts.MsgConst;
import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.cache.MqttRetainMessage;
import io.stuart.entities.cache.MqttRouter;
import io.stuart.ext.collections.BoundedConcurrentLinkedQueue;
import io.stuart.ext.collections.ExpiringMap;
import io.stuart.log.Logger;
import io.stuart.services.cache.CacheService;
import io.stuart.services.metrics.MetricsService;
import io.stuart.sessions.SessionWrapper;
import io.stuart.utils.CacheUtil;
import io.stuart.utils.IdUtil;
import io.stuart.utils.MsgUtil;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.messages.MqttPublishMessage;
import net.jodah.expiringmap.ExpirationListener;

// TODO optimizing young garbage collection time with off-heap cache(OHC)
public class TransientSessionWrapper implements SessionWrapper {

    private Vertx vertx;

    private CacheService cacheService;

    private MqttEndpoint endpoint;

    private String thisClientId;

    private ExpiringMap<Integer, MqttAwaitMessage> awaits;

    private BoundedConcurrentLinkedQueue<MqttMessage> queue;

    private ConcurrentHashMap<Integer, MqttMessage> inflights;

    private ExpiringMap<Integer, String> timeouts;

    private ExpirationListener<Integer, MqttAwaitMessage> awaitExpiredListener = (key, val) -> {
    };

    private ExpirationListener<Integer, String> inflightExpiredListener = (key, val) -> {
        vertx.executeBlocking(future -> {
            // retry: publish inflight message again
            retry(key);
            // complete the blocking code
            future.complete();
        }, false, result -> {
            // do nothing...
        });
    };

    private AtomicInteger queueCas;

    private AtomicInteger inflightCas;

    public TransientSessionWrapper(Vertx vertx, CacheService cacheService, MqttEndpoint endpoint) {
        this.vertx = vertx;
        this.cacheService = cacheService;
        this.endpoint = endpoint;
        this.thisClientId = endpoint.clientIdentifier();
        this.awaits = CacheUtil.expiringMap(Config.getSessionAwaitRelExpiryIntervalS(), awaitExpiredListener, Config.getSessionAwaitRelMaxCapacity());
        this.queue = new BoundedConcurrentLinkedQueue<>(Config.getSessionQueueMaxCapacity());
        this.inflights = new ConcurrentHashMap<>();
        this.timeouts = CacheUtil.expiringMap(Config.getSessionInflightExpiryIntervalS(), inflightExpiredListener, 0);
        this.queueCas = new AtomicInteger(0);
        this.inflightCas = new AtomicInteger(0);
    }

    @Override
    public void open() {
        // initialize transient session
        cacheService.initTransientSession(thisClientId);
    }

    @Override
    public void close() {
        if (endpoint != null && endpoint.isConnected()) {
            endpoint.close();
        }

        // destroy transient session
        cacheService.destroyTransientSession(thisClientId);
    }

    @Override
    public void closeEndpoint() {
        if (endpoint != null && endpoint.isConnected()) {
            endpoint.close();
        }
    }

    @Override
    public void refreshEndpoint(MqttEndpoint endpoint) {
        // do nothing...
    }

    @Override
    public boolean isSameEndpoint(MqttEndpoint compare) {
        if (endpoint == null || compare == null) {
            return false;
        }

        return endpoint.equals(compare);
    }

    @Override
    public void subscribeTopic(String topic, int qos) {
        // get router
        MqttRouter router = cacheService.getRouter(thisClientId, topic);

        if (router != null) {
            router.setQos(qos);
        } else {
            router = new MqttRouter();

            router.setClientId(thisClientId);
            router.setTopic(topic);
            router.setQos(qos);

            // metrics: session topic count + 1
            MetricsService.i().record(thisClientId, MetricsConst.PN_SSM_TOPIC_COUNT, 1);
        }

        // save router
        cacheService.saveRouter(router);
    }

    @Override
    public void unsubscribeTopic(String topic) {
        // delete router
        cacheService.deleteRouter(thisClientId, topic);

        // metrics: session topic count - 1
        MetricsService.i().record(thisClientId, MetricsConst.PN_SSM_TOPIC_COUNT, -1);
    }

    @Override
    public void receiveQos2Message(MqttPublishMessage message) {
        // get await message
        MqttAwaitMessage awaitMessage = MsgUtil.convert2MqttAwaitMessage(message, thisClientId);
        // save await message
        boolean result = awaits.putExt(awaitMessage.getMessageId(), awaitMessage);

        // metrics: set session await size
        recordAwaitSize();
        // metrics: save succeeded?
        if (!result) {
            // 1.metrics: session dropped count + 1
            // 2.metrics: mqtt message dropped count + 1
            MetricsService.i().grecord(thisClientId, MetricsConst.GPN_MESSAGE_DROPPED, 1);
        }
    }

    @Override
    public MqttAwaitMessage releaseQos2Message(int messageId) {
        // get and remove await message
        MqttAwaitMessage result = awaits.remove(messageId);

        // metrics: set session await size
        recordAwaitSize();
        // metrics: remove succeeded?
        if (result == null) {
            // metrics: mqtt 'PUBREL' missed count + 1
            MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBREL_MISSED, 1);
        }

        // return await message
        return result;
    }

    @Override
    public void publishMessage(MqttMessage message, int qos) {
        if (message == null) {
            return;
        }

        // get mqtt message
        MqttMessage mqttMessage = MsgUtil.copyMqttMessage(message);

        // set client id
        mqttMessage.setClientId(thisClientId);
        // set qos
        mqttMessage.setQos(qos);

        // handle mqtt message
        if (MsgConst.ENQUEUE_SUCCEEDED == handleMessage(mqttMessage)) {
            // publish cached message
            publishCachedMessage();
        }
    }

    @Override
    public void publishRetainMessage(List<MqttRetainMessage> retains) {
        if (retains == null || retains.isEmpty()) {
            return;
        }

        // mqtt message
        MqttMessage message = null;
        // enqueue count
        int count = 0;

        for (MqttRetainMessage retain : retains) {
            // get mqtt message
            message = MsgUtil.convert2MqttMessage(retain);

            // set client id
            message.setClientId(thisClientId);

            // handle mqtt message
            if (MsgConst.ENQUEUE_SUCCEEDED == handleMessage(message)) {
                ++count;
            }
        }

        if (count > 0) {
            // publish cached message
            publishCachedMessage();
        }
    }

    @Override
    public void publishCachedMessage() {
        dequeue();
    }

    @Override
    public void receivePuback(int messageId) {
        Logger.log().debug("clientId : {} - receive a 'PUBACK', then delete the inflight message(id = {}, qos = 1).", thisClientId, messageId);

        // delete inflight message
        boolean result = inflights.remove(messageId) == null;
        // delete inflight timeout
        deleteInflightTimeout(messageId);

        // metrics: set session inflight size
        recordInflightSize();
        // metrics: remove succeeded?
        if (result) {
            // metrics: mqtt 'PUBACK' missed count + 1
            MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBACK_MISSED, 1);
        }

        Logger.log().debug("clientId : {} - after delete an inflight message(id = {}, qos = 1), the inflight size = {}.", thisClientId, messageId,
                inflights.size());

        // publish cached message
        publishCachedMessage();
    }

    @Override
    public void receivePubrec(int messageId) {
        Logger.log().debug("clientId : {} - receive a 'PUBREC', then update the message(id = {}, qos = 2) status.", thisClientId, messageId);

        // get inflight message
        MqttMessage message = inflights.get(messageId);

        if (message != null) {
            // set inflight message status
            message.setStatus(MsgConst.PUBREC_STATUS);
        } else {
            // metrics: mqtt 'PUBREC' missed count + 1
            MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBREC_MISSED, 1);
        }
    }

    @Override
    public void receivePubcomp(int messageId) {
        Logger.log().debug("clientId : {} - receive a 'PUBCOMP', then delete the inflight message(id = {}, qos = 2).", thisClientId, messageId);

        // delete inflight message
        boolean result = inflights.remove(messageId) == null;
        // delete inflight timeout
        deleteInflightTimeout(messageId);

        // metrics: set session inflight size
        recordInflightSize();
        // metrics: remove succeeded?
        if (result) {
            // metrics: mqtt 'PUBCOMP' missed count + 1
            MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBCOMP_MISSED, 1);
        }

        Logger.log().debug("clientId : {} - after delete an inflight message(id = {}, qos = 2), the inflight size = {}.", thisClientId, messageId,
                inflights.size());

        // publish cached message
        publishCachedMessage();
    }

    @Override
    public void retry(int messageId) {
        // get inflight message
        MqttMessage message = inflights.get(messageId);

        if (message == null) {
            return;
        }

        Logger.log().debug("clientId : {} - receive an inflight message time to live event({}).", thisClientId, messageId);

        if (message.getRetry() >= Config.getSessionInflightMaxRetries()) {
            // delete inflight message
            inflights.remove(messageId);

            // metrics: set session inflight size
            recordInflightSize();
            // 1.metrics: session dropped count + 1
            // 2.metrics: mqtt message dropped count + 1
            MetricsService.i().grecord(thisClientId, MetricsConst.GPN_MESSAGE_DROPPED, 1);

            Logger.log().debug("clientId : {} - the message({}) reach the max retry time.", thisClientId, message);
            Logger.log().debug("clientId : {} - after delete an inflight message({}), the inflight size = {}.", thisClientId, message, inflights.size());

            // publish cached message
            publishCachedMessage();

            // finish retry
            return;
        }

        // retry inflight message
        retryInflight(message);
    }

    @Override
    public boolean isInflightFull() {
        return Config.getSessionInflightMaxCapacity() > 0 && inflights.size() >= Config.getSessionInflightMaxCapacity();
    }

    @Override
    public void saveInflightTimeout(int messageId) {
        timeouts.put(messageId, CacheConst.MESSAGE_EXPIRER_VALUE);
    }

    @Override
    public void deleteInflightTimeout(int messageId) {
        timeouts.remove(messageId);
    }

    private int handleMessage(MqttMessage message) {
        if (message == null) {
            return MsgConst.NULL_ERROR;
        }

        // get matched qos
        MqttQoS qos = message.publishQoS();

        if (qos == MqttQoS.AT_MOST_ONCE) {
            if (Config.isSessionQueueStoreQos0() && !queue.isEmpty()) {
                // enqueue message
                return enqueue(message);
            } else {
                // publish message
                return publish(message, false);
            }
        }

        // inflight message
        return inflight(message);
    }

    private int enqueue(MqttMessage message) {
        if (message == null) {
            return MsgConst.NULL_ERROR;
        }

        // add message to the end
        int result = queue.addExt(message);

        if (result >= 0) {
            if (result == 0) {
                // metrics: set session queue size
                recordQueueSize();
                // metrics: session enqueue count + 1
                MetricsService.i().record(thisClientId, MetricsConst.PN_SSM_ENQUEUE_COUNT, 1);
            } else {
                // 1.metrics: session enqueue count + 1
                // 2.metrics: session dropped count + 1
                // 3.metrics: mqtt message dropped count + 1
                MetricsService.i().grecord(thisClientId, MetricsConst.GPN_MESSAGE_ENQUEUE_AND_DROPPED, 1);
            }

            // return enqueue succeeded
            return MsgConst.ENQUEUE_SUCCEEDED;
        } else {
            // return enqueue failed
            return MsgConst.ENQUEUE_FAILED;
        }
    }

    private void dequeue() {
        if (!takeQueueCas()) {
            return;
        }

        // mqtt message
        MqttMessage message = null;

        for (;;) {
            if (endpoint == null || !endpoint.isConnected() || queue.isEmpty() || isInflightFull()) {
                break;
            }

            takeInflightCas();

            try {
                if (endpoint == null || !endpoint.isConnected() || queue.isEmpty() || isInflightFull()) {
                    break;
                }

                // poll the first message
                message = queue.poll();

                if (message == null) {
                    break;
                }

                // metrics: set session queue size
                recordQueueSize();

                if (message.publishQoS() == MqttQoS.AT_MOST_ONCE && publish(message, false) != 0) {
                    break;
                } else {
                    // get next message id
                    int messageId = IdUtil.nextMessageId(endpoint);

                    if (messageId > 0) {
                        // set message id
                        message.setMessageId(messageId);
                        // save new inflight message
                        inflights.put(messageId, message);
                        // save inflight timeout
                        saveInflightTimeout(messageId);

                        // metrics: set session inflight size
                        recordInflightSize();

                        // publish message and get result
                        messageId = publish(message, false);
                    } else {
                        break;
                    }
                }
            } finally {
                releaseInflightCas();
            }
        }

        releaseQueueCas();
    }

    private int inflight(MqttMessage message) {
        if (!queue.isEmpty() || isInflightFull()) {
            // enqueue message
            return enqueue(message);
        }

        // publish message result
        int result = MsgConst.PUBLISH_ERROR;

        takeInflightCas();

        try {
            if (!queue.isEmpty() || isInflightFull()) {
                // enqueue message
                result = enqueue(message);
            } else {
                // get next message id
                int messageId = IdUtil.nextMessageId(endpoint);

                if (messageId > 0) {
                    // set message id
                    message.setMessageId(messageId);

                    // save new inflight message
                    inflights.put(messageId, message);
                    // save inflight timeout
                    saveInflightTimeout(messageId);

                    // metrics: set session inflight size
                    recordInflightSize();

                    // publish message and get result
                    result = publish(message, false);

                    Logger.log().debug(
                            "clientId : {} - publish message to client, the inflight size = {}, get next message id = {} and publish message id = {}.",
                            thisClientId, inflights.size(), messageId, result);
                } else {
                    // enqueue message
                    result = enqueue(message);

                    Logger.log().debug("clientId : {} - get next message id({}) failed, then enqueue.", thisClientId, messageId);
                }
            }
        } finally {
            releaseInflightCas();
        }

        return result;
    }

    private int retryInflight(MqttMessage message) {
        // get old id
        int old = message.getMessageId();
        // get status
        int status = message.getStatus();

        if (MqttQoS.EXACTLY_ONCE == message.publishQoS() && MsgConst.PUBREC_STATUS == status) {
            // set message retry count
            message.setRetry(message.getRetry() + 1);
            // save inflight timeout
            saveInflightTimeout(old);

            Logger.log().debug("clientId : {} - after update inflight message({}) - wait release, the inflight size = {}.", thisClientId, message,
                    inflights.size());

            // publish 'PUBREL' to client
            return publishRelease(old);
        }

        // publish message result
        int result = MsgConst.PUBLISH_ERROR;

        takeInflightCas();

        try {
            // get next message id
            int messageId = IdUtil.nextMessageId(endpoint);

            if (messageId > 0) {
                // set message id
                message.setMessageId(messageId);
                // set message retry count
                message.setRetry(message.getRetry() + 1);

                // remove old inflight message
                inflights.remove(old);
                // save new inflight message
                inflights.put(messageId, message);
                // save inflight timeout
                saveInflightTimeout(messageId);

                // metrics: set session inflight size
                recordInflightSize();

                // publish message and get result
                result = publish(message, true);

                Logger.log().debug("clientId : {} - after add new inflight message({}) and delete the old one({}), the inflight size = {}.", thisClientId, old,
                        messageId, inflights.size());
            }
        } finally {
            releaseInflightCas();
        }

        return result;
    }

    private int publish(MqttMessage message, boolean everPublished) {
        if (endpoint == null || !endpoint.isConnected()) {
            return MsgConst.PUBLISH_ERROR;
        }

        // get message qos
        MqttQoS qos = message.publishQoS();

        try {
            if (everPublished) {
                // publish message to client
                endpoint.publish(message.getTopic(), message.publishPayload(), qos, true, message.isRetain());
            } else {
                // publish message to client
                endpoint.publish(message.getTopic(), message.publishPayload(), qos, message.isDup(), message.isRetain());
            }

            // 1.metrics: session sent count + 1
            // 2.metrics: mqtt 'PUBLISH' sent count + 1
            // 3.metrics: mqtt message(qos0/1/2) sent count + 1
            // 4.metrics: mqtt message sent bytes count + this message length
            MetricsService.i().grecord(thisClientId, MetricsConst.GPN_MESSAGE_SENT, qos.value(), message.getPayload().length);
        } catch (IllegalStateException closed) {
            return MsgConst.PUBLISH_ERROR;
        }

        if (MqttQoS.AT_MOST_ONCE == qos) {
            return 0;
        } else {
            return endpoint.lastMessageId();
        }
    }

    private int publishRelease(int messageId) {
        try {
            // publish 'PUBREL' to client
            endpoint.publishRelease(messageId);

            // metrics: mqtt 'PUBREL' sent count + 1
            MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBREL_SENT, 1);

            // return message id
            return messageId;
        } catch (IllegalStateException closed) {
            // return publish error
            return MsgConst.PUBLISH_ERROR;
        }
    }

    private boolean takeQueueCas() {
        return queueCas.compareAndSet(0, 1);
    }

    private void releaseQueueCas() {
        queueCas.set(0);
    }

    private boolean takeInflightCas() {
        for (;;) {
            if (inflightCas.compareAndSet(0, 1))
                return true;
        }
    }

    private void releaseInflightCas() {
        inflightCas.set(0);
    }

    private void recordAwaitSize() {
        // optimize: getting size wastes the cpu processing time
        if (MetricsService.i().isEnabled()) {
            // metrics: set session await size
            MetricsService.i().record(thisClientId, MetricsConst.PN_SSM_AWAIT_SIZE, awaits.size());
        }
    }

    private void recordQueueSize() {
        // optimize: getting size wastes the cpu processing time
        if (MetricsService.i().isEnabled()) {
            // metrics: set session queue size
            MetricsService.i().record(thisClientId, MetricsConst.PN_SSM_QUEUE_SIZE, queue.size());
        }
    }

    private void recordInflightSize() {
        // optimize: getting size wastes the cpu processing time
        if (MetricsService.i().isEnabled()) {
            // metrics: set session inflight size
            MetricsService.i().record(thisClientId, MetricsConst.PN_SSM_INFLIGHT_SIZE, inflights.size());
        }
    }

}
