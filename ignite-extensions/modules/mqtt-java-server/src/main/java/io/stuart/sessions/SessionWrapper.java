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

package io.stuart.sessions;

import java.util.List;

import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.cache.MqttRetainMessage;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.messages.MqttPublishMessage;

public interface SessionWrapper {

    void open();

    void close();

    void closeEndpoint();

    void refreshEndpoint(MqttEndpoint endpoint);

    boolean isSameEndpoint(MqttEndpoint compare);

    void subscribeTopic(String topic, int qos);

    void unsubscribeTopic(String topic);

    void receiveQos2Message(MqttPublishMessage message);

    MqttAwaitMessage releaseQos2Message(int messageId);

    void publishMessage(MqttMessage message, int qos);

    void publishRetainMessage(List<MqttRetainMessage> retains);

    void publishCachedMessage();

    void receivePuback(int messageId);

    void receivePubrec(int messageId);

    void receivePubcomp(int messageId);

    void retry(int messageId);

    boolean isInflightFull();

    void saveInflightTimeout(int messageId);

    void deleteInflightTimeout(int messageId);

}
