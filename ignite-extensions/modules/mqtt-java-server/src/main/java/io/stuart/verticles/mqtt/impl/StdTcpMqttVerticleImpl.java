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

package io.stuart.verticles.mqtt.impl;

import java.util.concurrent.atomic.AtomicInteger;

import io.stuart.config.Config;
import io.stuart.consts.SysConst;
import io.stuart.log.Logger;
import io.vertx.mqtt.MqttServerOptions;

public class StdTcpMqttVerticleImpl extends StdAbstractMqttVerticle {

    // initialize connection count
    private static final AtomicInteger tcpConnCount = new AtomicInteger(0);

    @Override
    public MqttServerOptions initOptions() {
        Logger.log().debug("Stuart initialize standalone mqtt server options for TCP protocol.");

        // set protocol
        protocol = SysConst.MQTT + SysConst.COLON + SysConst.TCP_PROTOCOL;
        // set port
        port = Config.getMqttPort();
        // set listener
        listener = Config.getInstanceListenAddr() + SysConst.COLON + Config.getMqttPort();
        // set max connections limit
        connMaxLimit = Config.getMqttMaxConns();

        // initialize mqtt server options
        MqttServerOptions options = new MqttServerOptions();

        // set mqtt server options
        options.setHost(Config.getInstanceListenAddr());
        options.setPort(Config.getMqttPort());
        options.setMaxMessageSize(Config.getMqttMessageMaxSize());
        options.setTimeoutOnConnect(Config.getMqttClientConnectTimeoutS());
        options.setUseWebSocket(false);

        // return mqtt server options
        return options;
    }

    @Override
    public boolean limited() {
        if (tcpConnCount.get() >= connMaxLimit) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int incrementAndGetConnCount() {
        return tcpConnCount.incrementAndGet();
    }

    @Override
    public int decrementAndGetConnCount() {
        return tcpConnCount.decrementAndGet();
    }

    @Override
    public int getConnCount() {
        return tcpConnCount.get();
    }

}
