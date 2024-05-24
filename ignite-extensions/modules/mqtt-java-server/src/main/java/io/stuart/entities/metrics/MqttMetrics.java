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

package io.stuart.entities.metrics;

import java.io.Serializable;
import java.util.concurrent.atomic.LongAdder;

import lombok.Getter;
import lombok.ToString;

@ToString
public class MqttMetrics implements Serializable {

    private static final long serialVersionUID = 7602482777448548414L;

    private static volatile MqttMetrics instance;

    // @Getter
    // private LongAdder packetReceived;

    // @Getter
    // private LongAdder packetSent;

    @Getter
    private LongAdder packetConnect;

    @Getter
    private LongAdder packetConnack;

    @Getter
    private LongAdder packetDisconnect;

    @Getter
    private LongAdder packetPingreq;

    @Getter
    private LongAdder packetPingresp;

    @Getter
    private LongAdder packetPublishReceived;

    @Getter
    private LongAdder packetPublishSent;

    @Getter
    private LongAdder packetPubackReceived;

    @Getter
    private LongAdder packetPubackSent;

    @Getter
    private LongAdder packetPubackMissed;

    @Getter
    private LongAdder packetPubcompReceived;

    @Getter
    private LongAdder packetPubcompSent;

    @Getter
    private LongAdder packetPubcompMissed;

    @Getter
    private LongAdder packetPubrecReceived;

    @Getter
    private LongAdder packetPubrecSent;

    @Getter
    private LongAdder packetPubrecMissed;

    @Getter
    private LongAdder packetPubrelReceived;

    @Getter
    private LongAdder packetPubrelSent;

    @Getter
    private LongAdder packetPubrelMissed;

    @Getter
    private LongAdder packetSubscribe;

    @Getter
    private LongAdder packetSuback;

    @Getter
    private LongAdder packetUnsubscribe;

    @Getter
    private LongAdder packetUnsuback;

    // @Getter
    // private LongAdder messageReceived;

    // @Getter
    // private LongAdder messageSent;

    @Getter
    private LongAdder messageDropped;

    @Getter
    private LongAdder messageQos0Received;

    @Getter
    private LongAdder messageQos0Sent;

    @Getter
    private LongAdder messageQos1Received;

    @Getter
    private LongAdder messageQos1Sent;

    @Getter
    private LongAdder messageQos2Received;

    @Getter
    private LongAdder messageQos2Sent;

    @Getter
    private LongAdder byteReceived;

    @Getter
    private LongAdder byteSent;

    private MqttMetrics() {
        // this.packetReceived = new LongAdder();
        // this.packetSent = new LongAdder();
        this.packetConnect = new LongAdder();
        this.packetConnack = new LongAdder();
        this.packetDisconnect = new LongAdder();
        this.packetPingreq = new LongAdder();
        this.packetPingresp = new LongAdder();
        this.packetPublishReceived = new LongAdder();
        this.packetPublishSent = new LongAdder();
        this.packetPubackReceived = new LongAdder();
        this.packetPubackSent = new LongAdder();
        this.packetPubackMissed = new LongAdder();
        this.packetPubcompReceived = new LongAdder();
        this.packetPubcompSent = new LongAdder();
        this.packetPubcompMissed = new LongAdder();
        this.packetPubrecReceived = new LongAdder();
        this.packetPubrecSent = new LongAdder();
        this.packetPubrecMissed = new LongAdder();
        this.packetPubrelReceived = new LongAdder();
        this.packetPubrelSent = new LongAdder();
        this.packetPubrelMissed = new LongAdder();
        this.packetSubscribe = new LongAdder();
        this.packetSuback = new LongAdder();
        this.packetUnsubscribe = new LongAdder();
        this.packetUnsuback = new LongAdder();
        // this.messageReceived = new LongAdder();
        // this.messageSent = new LongAdder();
        this.messageDropped = new LongAdder();
        this.messageQos0Received = new LongAdder();
        this.messageQos0Sent = new LongAdder();
        this.messageQos1Received = new LongAdder();
        this.messageQos1Sent = new LongAdder();
        this.messageQos2Received = new LongAdder();
        this.messageQos2Sent = new LongAdder();
        this.byteReceived = new LongAdder();
        this.byteSent = new LongAdder();
    }

    public static MqttMetrics getInstance() {
        if (instance == null) {
            synchronized (MqttMetrics.class) {
                if (instance == null) {
                    instance = new MqttMetrics();
                }
            }
        }

        return instance;
    }

}
