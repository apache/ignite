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

package io.stuart.verticles.mqtt;

import java.util.List;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.internal.MqttRoute;
import io.stuart.sessions.SessionWrapper;
import io.vertx.mqtt.MqttAuth;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubscribeMessage;
import io.vertx.mqtt.messages.MqttUnsubscribeMessage;

public interface MqttVerticle {

    MqttServerOptions initOptions();

    void handleEndpoint(MqttEndpoint endpoint);

    void handleReject(MqttEndpoint endpoint, MqttConnectReturnCode code);

    void handleAccept(MqttEndpoint endpoint);

    void handleSubscribe(MqttEndpoint endpoint, MqttSubscribeMessage subscribe);

    void handleUnsubscribe(MqttEndpoint endpoint, MqttUnsubscribeMessage unsubscribe);

    void handlePublish(MqttEndpoint endpoint, MqttPublishMessage message);

    void handlePublishRelease(MqttEndpoint endpoint, int messageId);

    void handlePublishAcknowledge(MqttEndpoint endpoint, int messageId);

    void handlePublishReceived(MqttEndpoint endpoint, int messageId);

    void handlePublishCompletion(MqttEndpoint endpoint, int messageId);

    void handlePing(MqttEndpoint endpoint);

    void handleDisconnect(MqttEndpoint endpoint);

    void handleClose(MqttEndpoint endpoint);

    void handleRetainMessage(MqttPublishMessage message);

    void handleWillMessage(MqttEndpoint endpoint);

    void handleWillPublish(MqttEndpoint endpoint);

    boolean limited();

    int incrementAndGetConnCount();

    int decrementAndGetConnCount();

    int getConnCount();

    boolean hasAuth(MqttAuth auth);

    void saveListener();

    void saveConnection(MqttEndpoint endpoint);

    void deleteConnection(MqttEndpoint endpoint);

    void handlePublishRetainMessage(SessionWrapper wrapper, String topic, int qos);

    void handleLocalPublishMessage(List<MqttRoute> routes, MqttPublishMessage message);

    void handleLocalPublishMessage(List<MqttRoute> routes, MqttMessage message);

    void handleLocalPublishMessage(MqttRoute route, MqttMessage message);

    void handleClusteredPublishMessage(List<MqttRoute> routes, MqttPublishMessage message);

    void handleClusteredPublishMessage(List<MqttRoute> routes, MqttMessage message);

    void handleClusteredPublishMessage(MqttRoute route, MqttMessage message);

}
