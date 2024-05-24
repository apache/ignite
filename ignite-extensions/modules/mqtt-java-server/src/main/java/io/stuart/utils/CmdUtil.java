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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import io.stuart.cli.StuartCLI;
import io.stuart.consts.CmdConst;
import io.stuart.consts.ParamConst;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CLIException;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;

public class CmdUtil {

    private static final Set<String> LEVELS = new HashSet<>();

    private static final Set<String> MODES = new HashSet<>();

    static {
        LEVELS.add(ParamConst.LOG_LEVEL_ALL.toLowerCase());
        LEVELS.add(ParamConst.LOG_LEVEL_DEBUG.toLowerCase());
        LEVELS.add(ParamConst.LOG_LEVEL_ERROR.toLowerCase());
        LEVELS.add(ParamConst.LOG_LEVEL_FATAL.toLowerCase());
        LEVELS.add(ParamConst.LOG_LEVEL_INFO.toLowerCase());
        LEVELS.add(ParamConst.LOG_LEVEL_OFF.toLowerCase());
        LEVELS.add(ParamConst.LOG_LEVEL_TRACE.toLowerCase());
        LEVELS.add(ParamConst.LOG_LEVEL_WARN.toLowerCase());

        MODES.add(ParamConst.CLUSTER_MODE_STD);
        MODES.add(ParamConst.CLUSTER_MODE_VMIP);
        MODES.add(ParamConst.CLUSTER_MODE_ZK);
    }

    public static CommandLine cli(String[] args) {
        CLI cli = new StuartCLI().setName(CmdConst.CLI_SHELL);
        cli.setSummary(CmdConst.CLI_SUMMARY);

        Option cfg = new Option();
        cfg.setLongName(CmdConst.CFG_L_NAME);
        cfg.setShortName(CmdConst.CFG_S_NAME);
        cfg.setDescription(CmdConst.CFG_DESC);
        cfg.setRequired(false);

        Option instanceId = new Option();
        instanceId.setLongName(CmdConst.INSTANCE_ID_L_NAME);
        instanceId.setShortName(CmdConst.INSTANCE_ID_S_NAME);
        instanceId.setDescription(CmdConst.INSTANCE_ID_DESC);
        instanceId.setRequired(false);

        Option listenAddr = new Option();
        listenAddr.setLongName(CmdConst.LISTEN_ADDR_L_NAME);
        listenAddr.setShortName(CmdConst.LISTEN_ADDR_S_NAME);
        listenAddr.setDescription(CmdConst.LISTEN_ADDR_DESC);
        listenAddr.setRequired(false);

        Option storagePath = new Option();
        storagePath.setLongName(CmdConst.STORAGE_PATH_L_NAME);
        storagePath.setShortName(CmdConst.STORAGE_PATH_S_NAME);
        storagePath.setDescription(CmdConst.STORAGE_PATH_DESC);
        storagePath.setRequired(false);

        Option logPath = new Option();
        logPath.setLongName(CmdConst.LOG_PATH_L_NAME);
        logPath.setShortName(CmdConst.LOG_PATH_S_NAME);
        logPath.setDescription(CmdConst.LOG_PATH_DESC);
        logPath.setRequired(false);

        Option logLevel = new Option();
        logLevel.setLongName(CmdConst.LOG_LEVEL_L_NAME);
        logLevel.setShortName(CmdConst.LOG_LEVEL_S_NAME);
        logLevel.setDescription(CmdConst.LOG_LEVEL_DESC);
        logLevel.setRequired(false);
        logLevel.setChoices(LEVELS);

        Option mqttPort = new Option();
        mqttPort.setLongName(CmdConst.MQTT_PORT_L_NAME);
        mqttPort.setShortName(CmdConst.MQTT_PORT_S_NAME);
        mqttPort.setDescription(CmdConst.MQTT_PORT_DESC);
        mqttPort.setRequired(false);

        Option mqttSslPort = new Option();
        mqttSslPort.setLongName(CmdConst.MQTT_SSL_PORT_L_NAME);
        mqttSslPort.setShortName(CmdConst.MQTT_SSL_PORT_S_NAME);
        mqttSslPort.setDescription(CmdConst.MQTT_SSL_PORT_DESC);
        mqttSslPort.setRequired(false);

        Option wsPort = new Option();
        wsPort.setLongName(CmdConst.WS_PORT_L_NAME);
        wsPort.setShortName(CmdConst.WS_PORT_S_NAME);
        wsPort.setDescription(CmdConst.WS_PORT_DESC);
        wsPort.setRequired(false);

        Option wssPort = new Option();
        wssPort.setLongName(CmdConst.WSS_PORT_L_NAME);
        wssPort.setShortName(CmdConst.WSS_PORT_S_NAME);
        wssPort.setDescription(CmdConst.WSS_PORT_DESC);
        wssPort.setRequired(false);

        Option httpPort = new Option();
        httpPort.setLongName(CmdConst.HTTP_PORT_L_NAME);
        httpPort.setShortName(CmdConst.HTTP_PORT_S_NAME);
        httpPort.setDescription(CmdConst.HTTP_PORT_DESC);
        httpPort.setRequired(false);

        Option clusterMode = new Option();
        clusterMode.setLongName(CmdConst.CLUSTER_MODE_L_NAME);
        clusterMode.setShortName(CmdConst.CLUSTER_MODE_S_NAME);
        clusterMode.setDescription(CmdConst.CLUSTER_MODE_DESC);
        clusterMode.setRequired(false);
        clusterMode.setChoices(MODES);

        Option vmipAddresses = new Option();
        vmipAddresses.setLongName(CmdConst.VMIP_ADDRESSES_L_NAME);
        vmipAddresses.setShortName(CmdConst.VMIP_ADDRESSES_S_NAME);
        vmipAddresses.setDescription(CmdConst.VMIP_ADDRESSES_DESC);
        vmipAddresses.setRequired(false);

        Option zkConnectString = new Option();
        zkConnectString.setLongName(CmdConst.ZK_CONNECT_STRING_L_NAME);
        zkConnectString.setShortName(CmdConst.ZK_CONNECT_STRING_S_NAME);
        zkConnectString.setDescription(CmdConst.ZK_CONNECT_STRING_DESC);
        zkConnectString.setRequired(false);

        Option zkRootPath = new Option();
        zkRootPath.setLongName(CmdConst.ZK_ROOT_PATH_L_NAME);
        zkRootPath.setShortName(CmdConst.ZK_ROOT_PATH_S_NAME);
        zkRootPath.setDescription(CmdConst.ZK_ROOT_PATH_DESC);
        zkRootPath.setRequired(false);

        Option zkReconnect = new Option();
        zkReconnect.setLongName(CmdConst.ZK_RECONNECT_ENABLE_L_NAME);
        zkReconnect.setShortName(CmdConst.ZK_RECONNECT_ENABLE_S_NAME);
        zkReconnect.setDescription(CmdConst.ZK_RECONNECT_ENABLE_DESC);
        zkReconnect.setRequired(false);
        zkReconnect.setChoices(new HashSet<String>(Arrays.asList(new String[] { "true", "false" })));

        cli.addOption(cfg);
        cli.addOption(instanceId);
        cli.addOption(listenAddr);
        cli.addOption(storagePath);
        cli.addOption(logPath);
        cli.addOption(logLevel);
        cli.addOption(mqttPort);
        cli.addOption(mqttSslPort);
        cli.addOption(wsPort);
        cli.addOption(wssPort);
        cli.addOption(httpPort);
        cli.addOption(clusterMode);
        cli.addOption(vmipAddresses);
        cli.addOption(zkConnectString);
        cli.addOption(zkRootPath);
        cli.addOption(zkReconnect);

        CommandLine commandLine = null;

        try {
            commandLine = cli.parse(Arrays.asList(args));
        } catch (CLIException e) {
            StringBuilder builder = new StringBuilder();
            cli.usage(builder);
            System.out.println(builder.toString());
        }

        return commandLine;
    }

}
