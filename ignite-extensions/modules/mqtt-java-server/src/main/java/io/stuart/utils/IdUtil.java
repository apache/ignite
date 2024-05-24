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

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;

import io.stuart.consts.MsgConst;
import io.vertx.mqtt.MqttEndpoint;

public class IdUtil {

    public static UUID uuid(String src) {
        if (StringUtils.isBlank(src)) {
            return null;
        }

        return UUID.fromString(src);
    }

    public static boolean validateClientId(String clientId) {
        if (clientId == null || clientId.length() == 0) {
            return false;
        }

        return true;
    }

    public static int nextMessageId(MqttEndpoint endpoint) {
        if (endpoint == null || !endpoint.isConnected()) {
            return -1;
        }

        int current = endpoint.lastMessageId();

        return ((current % MsgConst.MAX_MESSAGE_ID) != 0) ? current + 1 : 1;
    }

}
