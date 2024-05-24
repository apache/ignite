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

package io.stuart.utils;

import java.nio.charset.StandardCharsets;

import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.cache.MqttRetainMessage;
import io.stuart.entities.cache.MqttWillMessage;
import io.vertx.mqtt.MqttWill;
import io.vertx.mqtt.messages.MqttPublishMessage;

public class MsgUtil {

    public static MqttMessage copyMqttMessage(MqttMessage message) {
        if (message == null) {
            return null;
        }

        MqttMessage mqttMessage = new MqttMessage();

        mqttMessage.setClientId(message.getClientId());
        mqttMessage.setMessageId(message.getMessageId());
        mqttMessage.setTopic(message.getTopic());
        mqttMessage.setPayload(message.getPayload());
        mqttMessage.setQos(message.getQos());
        mqttMessage.setDup(message.isDup());
        mqttMessage.setRetain(message.isRetain());
        mqttMessage.setStatus(message.getStatus());
        mqttMessage.setRetry(message.getRetry());

        return mqttMessage;
    }

    public static MqttMessage convert2MqttMessage(MqttPublishMessage message) {
        if (message == null) {
            return null;
        }

        MqttMessage mqttMessage = new MqttMessage();

        mqttMessage.setTopic(message.topicName());
        mqttMessage.setPayload(message.payload().getBytes());
        mqttMessage.setQos(message.qosLevel().value());
        mqttMessage.setDup(message.isDup());
        mqttMessage.setRetain(message.isRetain());

        return mqttMessage;
    }

    public static MqttMessage convert2MqttMessage(MqttAwaitMessage message) {
        if (message == null) {
            return null;
        }

        MqttMessage mqttMessage = new MqttMessage();

        mqttMessage.setTopic(message.getTopic());
        mqttMessage.setPayload(message.getPayload());
        mqttMessage.setQos(message.getQos());
        mqttMessage.setDup(message.isDup());
        mqttMessage.setRetain(message.isRetain());

        return mqttMessage;
    }

    public static MqttMessage convert2MqttMessage(MqttRetainMessage message) {
        if (message == null) {
            return null;
        }

        MqttMessage mqttMessage = new MqttMessage();

        mqttMessage.setTopic(message.getTopic());
        mqttMessage.setPayload(message.getPayload());
        mqttMessage.setQos(message.getQos());
        mqttMessage.setDup(message.isDup());
        mqttMessage.setRetain(true);

        return mqttMessage;
    }

    public static MqttMessage convert2MqttMessage(MqttWillMessage message) {
        if (message == null) {
            return null;
        }

        MqttMessage mqttMessage = new MqttMessage();

        mqttMessage.setTopic(message.getTopic());
        mqttMessage.setPayload(message.getPayload().getBytes());
        mqttMessage.setQos(message.getQos());
        mqttMessage.setDup(false);
        mqttMessage.setRetain(message.isRetain());

        return mqttMessage;
    }

    public static MqttAwaitMessage convert2MqttAwaitMessage(MqttPublishMessage message, String clientId) {
        if (message == null || !IdUtil.validateClientId(clientId)) {
            return null;
        }

        MqttAwaitMessage awaitMessage = new MqttAwaitMessage();

        awaitMessage.setClientId(clientId);
        awaitMessage.setMessageId(message.messageId());
        awaitMessage.setTopic(message.topicName());
        awaitMessage.setPayload(message.payload().getBytes());
        awaitMessage.setQos(message.qosLevel().value());
        awaitMessage.setDup(message.isDup());
        awaitMessage.setRetain(message.isRetain());

        return awaitMessage;
    }

    public static MqttRetainMessage convert2MqttRetainMessage(MqttPublishMessage message) {
        MqttRetainMessage retainMessage = new MqttRetainMessage();

        retainMessage.setTopic(message.topicName());
        retainMessage.setPayload(message.payload().getBytes());
        retainMessage.setQos(message.qosLevel().value());
        retainMessage.setDup(message.isDup());

        return retainMessage;
    }

    public static MqttWillMessage convert2MqttWillMessage(MqttWill message, String clientId) {
        MqttWillMessage willMessage = new MqttWillMessage();

        willMessage.setClientId(clientId);
        willMessage.setTopic(message.getWillTopic());
        willMessage.setPayload(message.getWillMessage());
        willMessage.setQos(message.getWillQos());
        willMessage.setRetain(message.isWillRetain());

        return willMessage;
    }

}
