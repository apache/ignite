package io.stuart.verticles.mqtt.impl;

import java.util.concurrent.atomic.AtomicInteger;

import io.stuart.config.Config;
import io.stuart.consts.SysConst;
import io.stuart.log.Logger;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.mqtt.MqttServerOptions;

public class ClsWssMqttVerticleImpl extends ClsAbstractMqttVerticle {

    // initialize connection count
    private static final AtomicInteger wssConnCount = new AtomicInteger(0);

    @Override
    public MqttServerOptions initOptions() {
        Logger.log().debug("Stuart initialize clustered mqtt server options for SSL WebSocket protocol.");

        // set protocol
        protocol = SysConst.MQTT + SysConst.COLON + SysConst.SSL_WEBSOCKET_PROTOCOL;
        // set port
        port = Config.getWssPort();
        // set listener
        listener = Config.getInstanceListenAddr() + SysConst.COLON + Config.getWssPort();
        // set max connections limit
        connMaxLimit = Config.getWssMaxConns();

        // initialize mqtt server options
        MqttServerOptions options = new MqttServerOptions();

        // set mqtt server options
        options.setHost(Config.getInstanceListenAddr());
        options.setPort(Config.getWssPort());
        options.setMaxMessageSize(Config.getMqttMessageMaxSize());
        options.setTimeoutOnConnect(Config.getMqttClientConnectTimeoutS());
        options.setKeyCertOptions(new PemKeyCertOptions().setKeyPath(Config.getMqttSslKeyPath()).setCertPath(Config.getMqttSslCertPath()));
        options.setSsl(true);
        options.setUseWebSocket(true);
        //options.setWebsocketPath(Config.getWssPath());
        options.setWebSocketAllowServerNoContext(true);
        options.setWebSocketPreferredClientNoContext(true);

        // return mqtt server options
        return options;
    }

    @Override
    public boolean limited() {
        if (wssConnCount.get() >= connMaxLimit) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int incrementAndGetConnCount() {
        return wssConnCount.incrementAndGet();
    }

    @Override
    public int decrementAndGetConnCount() {
        return wssConnCount.decrementAndGet();
    }

    @Override
    public int getConnCount() {
        return wssConnCount.get();
    }

}
