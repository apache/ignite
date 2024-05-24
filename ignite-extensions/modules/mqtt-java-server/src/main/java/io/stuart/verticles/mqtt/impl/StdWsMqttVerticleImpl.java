package io.stuart.verticles.mqtt.impl;

import java.util.concurrent.atomic.AtomicInteger;

import io.stuart.config.Config;
import io.stuart.consts.SysConst;
import io.stuart.log.Logger;
import io.vertx.mqtt.MqttServerOptions;

public class StdWsMqttVerticleImpl extends StdAbstractMqttVerticle {

    // initialize connection count
    private static final AtomicInteger wsConnCount = new AtomicInteger(0);

    @Override
    public MqttServerOptions initOptions() {
        Logger.log().debug("Stuart initialize standalone mqtt server options for WebSocket protocol.");

        // set protocol
        protocol = SysConst.MQTT + SysConst.COLON + SysConst.WEBSOCKET_PROTOCOL;
        // set port
        port = Config.getWsPort();
        // set listener
        listener = Config.getInstanceListenAddr() + SysConst.COLON + Config.getWsPort();
        // set max connections limit
        connMaxLimit = Config.getWsMaxConns();

        // initialize mqtt server options
        MqttServerOptions options = new MqttServerOptions();

        // set mqtt server options
        options.setHost(Config.getInstanceListenAddr());
        options.setPort(Config.getWsPort());
        options.setMaxMessageSize(Config.getMqttMessageMaxSize());
        options.setTimeoutOnConnect(Config.getMqttClientConnectTimeoutS());
        options.setUseWebSocket(true);
        options.setWebSocketAllowServerNoContext(true);
        options.setWebSocketPreferredClientNoContext(true);

        // return mqtt server options
        return options;
    }

    @Override
    public boolean limited() {
        if (wsConnCount.get() >= connMaxLimit) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int incrementAndGetConnCount() {
        return wsConnCount.incrementAndGet();
    }

    @Override
    public int decrementAndGetConnCount() {
        return wsConnCount.decrementAndGet();
    }

    @Override
    public int getConnCount() {
        return wsConnCount.get();
    }

}
