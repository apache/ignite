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

import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.config.Config;

public class QosUtil {

    public static int calculate(int messageQos, int topicQos) {
        if (Config.isSessionUpgradeQos()) {
            return upgrade(messageQos, topicQos);
        } else {
            return downgrade(messageQos, topicQos);
        }
    }

    public static MqttQoS calculate(MqttQoS messageQos, MqttQoS topicQos) {
        if (Config.isSessionUpgradeQos()) {
            return upgrade(messageQos, topicQos);
        } else {
            return downgrade(messageQos, topicQos);
        }
    }

    private static int downgrade(int messageQos, int topicQos) {
        return messageQos < topicQos ? messageQos : topicQos;
    }

    private static int upgrade(int messageQos, int topicQos) {
        return messageQos > topicQos ? messageQos : topicQos;
    }

    private static MqttQoS downgrade(MqttQoS messageQos, MqttQoS topicQos) {
        if (messageQos == null || topicQos == null) {
            return null;
        }

        // get message qos int value
        int iMessageQos = messageQos.value();
        // get topic qos int value
        int iTopicQos = topicQos.value();

        return MqttQoS.valueOf(iMessageQos < iTopicQos ? iMessageQos : iTopicQos);
    }

    private static MqttQoS upgrade(MqttQoS messageQos, MqttQoS topicQos) {
        if (messageQos == null || topicQos == null) {
            return null;
        }

        // get message qos int value
        int iMessageQos = messageQos.value();
        // get topic qos int value
        int iTopicQos = topicQos.value();

        return MqttQoS.valueOf(iMessageQos > iTopicQos ? iMessageQos : iTopicQos);
    }

}
