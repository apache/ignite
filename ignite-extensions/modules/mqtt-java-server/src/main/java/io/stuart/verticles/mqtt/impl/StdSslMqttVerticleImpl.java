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
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.mqtt.MqttServerOptions;

public class StdSslMqttVerticleImpl extends StdAbstractMqttVerticle {

    // initialize connection count
    private static final AtomicInteger sslConnCount = new AtomicInteger(0);

    @Override
    public MqttServerOptions initOptions() {
        Logger.log().debug("Stuart initialize standalone mqtt server options for SSL protocol.");

        // set protocol
        protocol = SysConst.MQTT + SysConst.COLON + SysConst.SSL_PROTOCOL;
        // set port
        port = Config.getMqttSslPort();
        // set listener
        listener = Config.getInstanceListenAddr() + SysConst.COLON + Config.getMqttSslPort();
        // set max connections limit
        connMaxLimit = Config.getMqttSslMaxConns();

        // initialize mqtt server options
        MqttServerOptions options = new MqttServerOptions();

        // set mqtt server options
        options.setHost(Config.getInstanceListenAddr());
        options.setPort(Config.getMqttSslPort());
        options.setMaxMessageSize(Config.getMqttMessageMaxSize());
        options.setTimeoutOnConnect(Config.getMqttClientConnectTimeoutS());
        options.setKeyCertOptions(new PemKeyCertOptions().setKeyPath(Config.getMqttSslKeyPath()).setCertPath(Config.getMqttSslCertPath()));
        options.setSsl(true);
        options.setUseWebSocket(false);

        // return mqtt server options
        return options;
    }

    @Override
    public boolean limited() {
        if (sslConnCount.get() >= connMaxLimit) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int incrementAndGetConnCount() {
        return sslConnCount.incrementAndGet();
    }

    @Override
    public int decrementAndGetConnCount() {
        return sslConnCount.decrementAndGet();
    }

    @Override
    public int getConnCount() {
        return sslConnCount.get();
    }

}
