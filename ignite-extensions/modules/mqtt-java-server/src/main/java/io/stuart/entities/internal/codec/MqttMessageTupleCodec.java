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

package io.stuart.entities.internal.codec;

import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.internal.MqttMessageTuple;
import io.stuart.entities.internal.MqttRoute;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class MqttMessageTupleCodec implements MessageCodec<MqttMessageTuple, MqttMessageTuple> {

    @Override
    public void encodeToWire(Buffer buffer, MqttMessageTuple tuple) {
        // get json object
        JsonObject jsonToEncode = JsonObject.mapFrom(tuple.getRoute());
        // get json string
        jsonToEncode.writeToBuffer(buffer);

        JsonObject jsonToEncodeBody = JsonObject.mapFrom(tuple.getMessage());

        jsonToEncodeBody.writeToBuffer(buffer);

        // get json string length
        //- int length = jsonToStr.getBytes().length;

        // append json string length
        //- buffer.appendInt(length);
        // append json string
        //- buffer.appendString(jsonToStr);
    }

    @Override
    public MqttMessageTuple decodeFromWire(int pos, Buffer buffer) {
        MqttMessageTuple msg = new MqttMessageTuple();
        MqttRoute route = new MqttRoute();

        // get json object
        JsonObject contentJson = new JsonObject();
        pos = contentJson.readFromBuffer(pos,buffer);
        msg.setRoute(contentJson.mapTo(MqttRoute.class));

        JsonObject contentJsonBody = new JsonObject();
        pos = contentJsonBody.readFromBuffer(pos,buffer);
        msg.setMessage(contentJson.mapTo(MqttMessage.class));
        return msg;
    }

    @Override
    public MqttMessageTuple transform(MqttMessageTuple tuple) {
        return tuple;
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }

}
