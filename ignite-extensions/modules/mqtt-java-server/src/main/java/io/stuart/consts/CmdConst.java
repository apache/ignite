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

public interface CmdConst {

    static final String CLI_SHELL = "java -jar <stuart>-<version>-fat.jar";

    static final String CLI_SUMMARY = "Stuart: MQTT Server.";

    static final String CFG_L_NAME = "cfg";

    static final String CFG_S_NAME = "c";

    static final String CFG_DESC = "The config file (in properties format).";

    static final String INSTANCE_ID_L_NAME = "instance-id";

    static final String INSTANCE_ID_S_NAME = "i";

    static final String INSTANCE_ID_DESC = "The instance identity. If server is in cluster mode, it must be unique.";

    static final String LISTEN_ADDR_L_NAME = "listen-address";

    static final String LISTEN_ADDR_S_NAME = "a";

    static final String LISTEN_ADDR_DESC = "The listen address.";

    static final String STORAGE_PATH_L_NAME = "storage-path";

    static final String STORAGE_PATH_S_NAME = "s";

    static final String STORAGE_PATH_DESC = "The local storage path.";

    static final String LOG_PATH_L_NAME = "log-path";

    static final String LOG_PATH_S_NAME = "l";

    static final String LOG_PATH_DESC = "The local log path.";

    static final String LOG_LEVEL_L_NAME = "log-level";

    static final String LOG_LEVEL_S_NAME = "ll";

    static final String LOG_LEVEL_DESC = "The log level.";

    static final String MQTT_PORT_L_NAME = "mqtt-port";

    static final String MQTT_PORT_S_NAME = "m";

    static final String MQTT_PORT_DESC = "The mqtt listen port (default value is 1883).";

    static final String MQTT_SSL_PORT_L_NAME = "mqtt-ssl-port";

    static final String MQTT_SSL_PORT_S_NAME = "ms";

    static final String MQTT_SSL_PORT_DESC = "The mqtt ssl listen port (default value is 8883).";

    static final String WS_PORT_L_NAME = "ws-port";

    static final String WS_PORT_S_NAME = "w";

    static final String WS_PORT_DESC = "The websocket listen port (default value is 8080).";

    static final String WSS_PORT_L_NAME = "wss-port";

    static final String WSS_PORT_S_NAME = "ws";

    static final String WSS_PORT_DESC = "The websocket listen port (default value is 8083).";

    static final String HTTP_PORT_L_NAME = "http-port";

    static final String HTTP_PORT_S_NAME = "h";

    static final String HTTP_PORT_DESC = "The http listen port (defalt value is 18083).";

    static final String CLUSTER_MODE_L_NAME = "cluster-mode";

    static final String CLUSTER_MODE_S_NAME = "cm";

    static final String CLUSTER_MODE_DESC = "The cluster mode.";

    static final String VMIP_ADDRESSES_L_NAME = "vmip-addresses";

    static final String VMIP_ADDRESSES_S_NAME = "va";

    static final String VMIP_ADDRESSES_DESC = "The vmip addresses.";

    static final String ZK_CONNECT_STRING_L_NAME = "zookeeper-connect-string";

    static final String ZK_CONNECT_STRING_S_NAME = "zc";

    static final String ZK_CONNECT_STRING_DESC = "The zookeeper connect string.";

    static final String ZK_ROOT_PATH_L_NAME = "zookeeper-root-path";

    static final String ZK_ROOT_PATH_S_NAME = "zp";

    static final String ZK_ROOT_PATH_DESC = "The root path of cluster in zookeeper.";

    static final String ZK_RECONNECT_ENABLE_L_NAME = "zookeeper-reconnect";

    static final String ZK_RECONNECT_ENABLE_S_NAME = "zr";

    static final String ZK_RECONNECT_ENABLE_DESC = "The zookeeper reconnect enabled(true/false).";

}
