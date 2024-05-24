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

package io.stuart.consts;

import io.netty.handler.codec.mqtt.MqttMessageType;

public interface MsgConst {

    static final int MAX_MESSAGE_ID = 65535;

    static final int PUB_STATUS = MqttMessageType.PUBLISH.value();

    static final int PUBREC_STATUS = MqttMessageType.PUBREC.value();;

    static final int NULL_ERROR = -100;

    static final int PUBLISH_ERROR = -99;

    static final int ENQUEUE_FAILED = -2;

    static final int ENQUEUE_SUCCEEDED = -1;

}
