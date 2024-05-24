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

package io.stuart.config;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.StringUtils;

import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.digest.DigestAlgorithm;
import cn.hutool.crypto.digest.Digester;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;
import io.stuart.consts.CmdConst;
import io.stuart.consts.ParamConst;
import io.stuart.consts.PropConst;
import io.stuart.props.Props;
import io.stuart.utils.DirUtil;
import io.stuart.utils.LogUtil;
import io.vertx.core.cli.CommandLine;
import lombok.Getter;

public class Config {

    private static Props props;

    @Getter
    private static String instanceId = ParamConst.INSTANCE_ID;

    @Getter
    private static String instanceListenAddr = ParamConst.INSTANCE_LISTEN_ADDR;

    @Getter
    private static long instanceMetricsPeriodMs = ParamConst.INSTANCE_METRICS_PERIOD_MS;

    @Getter
    private static String storageDir = ParamConst.STORAGE_DIR;

    @Getter
    private static String storageDataDir;

    @Getter
    private static String storageWalDir;

    @Getter
    private static String storageWalArchiveDir;

    @Getter
    private static String storageWriteSyncMode = ParamConst.STORAGE_WRITE_SYNC_MODE_PRIMARY_SYNC;

    @Getter
    private static String storageWalMode = ParamConst.STORAGE_WAL_MODE_LOG_ONLY;

    @Getter
    private static int storageWalFlushFrequencyMs = ParamConst.STORAGE_WAL_FLUSH_FREQUENCY_MS;

    @Getter
    private static String logDir = ParamConst.LOG_DIR;

    @Getter
    private static String logLevel = ParamConst.LOG_LEVEL_INFO;

    @Getter
    private static Integer mqttPort = ParamConst.MQTT_PORT;

    @Getter
    private static Integer mqttSslPort = ParamConst.MQTT_SSL_PORT;

    @Getter
    private static Integer wsPort = ParamConst.WS_PORT;

    @Getter
    private static String wsPath = ParamConst.WS_PATH;

    @Getter
    private static Integer wssPort = ParamConst.WSS_PORT;

    @Getter
    private static String wssPath = ParamConst.WSS_PATH;

    @Getter
    private static Integer httpPort = ParamConst.HTTP_PORT;

    @Getter
    private static Integer mqttMaxConns = ParamConst.MQTT_MAX_CONNECTIONS;

    @Getter
    private static Integer mqttSslMaxConns = ParamConst.MQTT_SSL_MAX_CONNECTIONS;

    @Getter
    private static Integer wsMaxConns = ParamConst.WS_MAX_CONNECTIONS;

    @Getter
    private static Integer wssMaxConns = ParamConst.WSS_MAX_CONNECTIONS;

    @Getter
    private static int mqttClientIdMaxLen = ParamConst.MQTT_CLIENT_ID_MAX_LEN;

    @Getter
    private static int mqttClientConnectTimeoutS = ParamConst.MQTT_CLIENT_CONNECT_TIMEOUT_S;

    @Getter
    private static int mqttClientIdleTimeoutS = ParamConst.MQTT_CLIENT_IDLE_TIMEOUT_S;

    @Getter
    private static int mqttPacketMaxSize = ParamConst.MQTT_PACKET_MAX_SIZE;

    @Getter
    private static int mqttMessageMaxSize = ParamConst.MQTT_PACKET_MAX_SIZE - ParamConst.MQTT_FIXED_HEADER_MIN_SIZE;

    @Getter
    private static int mqttRetainMaxCapacity = ParamConst.MQTT_RETAIN_MAX_CAPACITY;

    @Getter
    private static int mqttRetainMaxPayload = ParamConst.MQTT_RETAIN_MAX_PAYLOAD;

    @Getter
    private static long mqttRetainExpiryIntervalS = ParamConst.MQTT_RETAIN_EXPIRY_INTERVAL_S;

    @Getter
    private static boolean mqttSslEnable = ParamConst.MQTT_SSL_ENABLE;

    @Getter
    private static String mqttSslKeyPath;

    @Getter
    private static String mqttSslCertPath;

    @Getter
    private static boolean mqttMetricsEnable = ParamConst.MQTT_METRICS_ENABLE;

    @Getter
    private static boolean sessionUpgradeQos = ParamConst.SESSION_UPGRADE_QOS;

    @Getter
    private static int sessionAwaitRelMaxCapacity = ParamConst.SESSION_AWAIT_REL_MAX_CAPACITY;

    @Getter
    private static long sessionAwaitRelExpiryIntervalS = ParamConst.SESSION_AWAIT_REL_EXPIRY_INTERVAL_S;

    @Getter
    private static int sessionQueueMaxCapacity = ParamConst.SESSION_QUEUE_MAX_CAPACITY;

    @Getter
    private static boolean sessionQueueStoreQos0 = ParamConst.SESSION_QUEUE_STORE_QOS0;

    @Getter
    private static int sessionInflightMaxCapacity = ParamConst.SESSION_INFLIGHT_MAX_CAPACITY;

    @Getter
    private static long sessionInflightExpiryIntervalS = ParamConst.SESSION_INFLIGHT_EXPIRY_INTERVAL_S;

    @Getter
    private static int sessionInflightMaxRetries = ParamConst.SESSION_INFLIGHT_MAX_RETRIES;

    @Getter
    private static AES aes;

    @Getter
    private static boolean authAllowAnonymous = ParamConst.AUTH_ALLOW_ANONYMOUS;

    @Getter
    private static boolean authAclAllowNomatch = ParamConst.AUTH_ACL_ALLOW_NOMATCH;

    @Getter
    private static String authMode = ParamConst.AUTH_MODE;

    @Getter
    private static String authRedisHost;

    @Getter
    private static int authRedisPort = ParamConst.AUTH_REDIS_PORT;

    @Getter
    private static String authRedisPass;

    @Getter
    private static int authRedisSelect = ParamConst.AUTH_REDIS_SELECT;

    @Getter
    private static String authRedisUserKeyPrefix = ParamConst.AUTH_REDIS_USER_KEY_PREFIX;

    @Getter
    private static String authRedisPasswdField = ParamConst.AUTH_REDIS_PASSWD_FIELD;

    @Getter
    private static String authRedisAclUserKeyPrefix = ParamConst.AUTH_REDIS_ACL_USER_KEY_PREFIX;

    @Getter
    private static String authRedisAclIpAddrKeyPrefix = ParamConst.AUTH_REDIS_ACL_IPADDR_KEY_PREFIX;

    @Getter
    private static String authRedisAclClientKeyPrefix = ParamConst.AUTH_REDIS_ACL_CLIENT_KEY_PREFIX;

    @Getter
    private static String authRedisAclAllKeyPrefix = ParamConst.AUTH_REDIS_ACL_ALL_KEY_PREFIX;

    @Getter
    private static String authRdbHost;

    @Getter
    private static int authRdbPort;

    @Getter
    private static String authRdbUsername;

    @Getter
    private static String authRdbPassword;

    @Getter
    private static String authRdbDatabase;

    @Getter
    private static String authRdbCharset = ParamConst.AUTH_RDB_CHARSET;

    @Getter
    private static int authRdbMaxPoolSize = ParamConst.AUTH_RDB_MAX_POOL_SIZE;

    @Getter
    private static int authRdbQueryTimeoutMs = ParamConst.AUTH_RDB_QUERY_TIMEOUT_MS;

    @Getter
    private static String authMongoHost;

    @Getter
    private static int authMongoPort = ParamConst.AUTH_MONGO_PORT;

    @Getter
    private static String authMongoDbName;

    @Getter
    private static String authMongoUsername;

    @Getter
    private static String authMongoPassword;

    @Getter
    private static String authMongoAuthSource;

    @Getter
    private static String authMongoAuthMechanism = ParamConst.AUTH_MONGO_AUTH_MECHANISM;

    @Getter
    private static int authMongoMaxPoolSize = ParamConst.AUTH_MONGO_MAX_POOL_SIZE;

    @Getter
    private static int authMongoMinPoolSize = ParamConst.AUTH_MONGO_MIN_POOL_SIZE;

    @Getter
    private static long authMongoMaxIdleTimeMs = ParamConst.AUTH_MONGO_MAX_IDLE_TIME_MS;

    @Getter
    private static long authMongoMaxLifeTimeMs = ParamConst.AUTH_MONGO_MAX_LIFE_TIME_MS;

    @Getter
    private static int authMongoWaitQueueMultiple = ParamConst.AUTH_MONGO_WAIT_QUEUE_MULTIPLE;

    @Getter
    private static long authMongoWaitQueueTimeoutMs = ParamConst.AUTH_MONGO_WAIT_QUEUE_TIMEOUT_MS;

    @Getter
    private static long authMongoMaintenanceFrequencyMs = ParamConst.AUTH_MONGO_MAINTENANCE_FREQUENCY_MS;

    @Getter
    private static long authMongoMaintenanceInitialDelayMs = ParamConst.AUTH_MONGO_MAINTENANCE_INITIAL_DELAY_MS;

    @Getter
    private static int authMongoConnectTimeoutMs = ParamConst.AUTH_MONGO_CONNECT_TIMEOUT_MS;

    @Getter
    private static int authMongoSocketTimeoutMs = ParamConst.AUTH_MONGO_SOCKET_TIMEOUT_MS;

    @Getter
    private static String authMongoUser = ParamConst.AUTH_MONGO_USER;

    @Getter
    private static String authMongoUserUsernameField = ParamConst.AUTH_MONGO_USER_USERNAME_FIELD;

    @Getter
    private static String authMongoUserPasswordField = ParamConst.AUTH_MONGO_USER_PASSWORD_FIELD;

    @Getter
    private static String authMongoAcl = ParamConst.AUTH_MONGO_ACL;

    @Getter
    private static String authMongoAclTargetField = ParamConst.AUTH_MONGO_ACL_TARGET_FIELD;

    @Getter
    private static String authMongoAclTypeField = ParamConst.AUTH_MONGO_ACL_TYPE_FIELD;

    @Getter
    private static String authMongoAclSeqField = ParamConst.AUTH_MONGO_ACL_SEQ_FIELD;

    @Getter
    private static String authMongoAclTopicsField = ParamConst.AUTH_MONGO_ACL_TOPICS_FIELD;

    @Getter
    private static String authMongoAclTopicField = ParamConst.AUTH_MONGO_ACL_TOPIC_FIELD;

    @Getter
    private static String authMongoAclAuthorityField = ParamConst.AUTH_MONGO_ACL_AUTHORITY_FIELD;

    @Getter
    private static boolean vertxMultiInstancesEnable = ParamConst.VERTX_MULTI_INSTANCES_ENABLE;

    @Getter
    private static int vertxMultiInstances = ParamConst.VERTX_MULTI_INSTANCES;

    @Getter
    private static int vertxWorkerPoolSize = ParamConst.VERTX_WORKER_POOL_SIZE;

    @Getter
    private static boolean vertxFileCachingEnabled = ParamConst.VERTX_FILE_CACHING_ENABLED;

    @Getter
    private static long vertxHttpSessionTimeoutMs = ParamConst.VERTX_HTTP_SESSION_TIMEOUT_MS;

    @Getter
    private static String clusterMode;

    @Getter
    private static int clusterStorageBackups = ParamConst.CLUSTER_STORAGE_BACKUPS;

    @Getter
    private static int clusterBltRebalanceTimeMs = ParamConst.CLSUTER_BLT_REBALANCE_TIME_MS;

    @Getter
    private static String vmipAddresses;

    @Getter
    private static String zkConnectString;

    @Getter
    private static String zkRootPath = ParamConst.ZK_ROOT_PATH;

    @Getter
    private static long zkJoinTimeoutMs = ParamConst.ZK_JOIN_TIMEOUT_MS;

    @Getter
    private static long zkSessionTimeoutMs = ParamConst.ZK_SESSION_TIMEOUT_MS;

    @Getter
    private static boolean zkReconnectEnable = ParamConst.ZK_RECONNECT_ENABLE;

    public static void init(CommandLine cmd) {
        // initialize properties
        props = props(cmd.getOptionValue(CmdConst.CFG_L_NAME), StandardCharsets.UTF_8);

        // initialize instance configurations
        instance(cmd);

        // initialize port configurations
        port(cmd);

        // initialize mqtt configurations
        mqtt(cmd);

        // initialize session configurations
        session(cmd);

        // initialize authorization and authentication configurations
        auth(cmd);

        // initialize vertx configurations
        vertx(cmd);

        // initialize cluster configurations
        cluster(cmd);
    }

    private static Props props(String path, Charset charset) {
        Props props = null;

        File cfg = null;

        if (StringUtils.isNotBlank(path)) {
            cfg = new File(path);
        }

        if (cfg != null && cfg.exists()) {
            props = new Props(cfg, charset);
        } else {
            props = new Props(ParamConst.CFG_PATH, charset);
        }

        return props;
    }

    private static void instance(CommandLine cmd) {
        // get instance id
        instanceId = cmd.getOptionValue(CmdConst.INSTANCE_ID_L_NAME);
        if (StringUtils.isBlank(instanceId)) {
            instanceId = props.getStr(PropConst.INSTANCE_ID, ParamConst.INSTANCE_ID);
        }

        // get instance listen address
        instanceListenAddr = cmd.getOptionValue(CmdConst.LISTEN_ADDR_L_NAME);
        if (StringUtils.isBlank(instanceListenAddr)) {
            instanceListenAddr = props.getStr(PropConst.INSTANCE_LISTEN_ADDR, ParamConst.INSTANCE_LISTEN_ADDR);
        }

        // get instance metrics period
        instanceMetricsPeriodMs = props.getLong(PropConst.INSTANCE_METRICS_PERIOD_MS, ParamConst.INSTANCE_METRICS_PERIOD_MS);

        // get storage directory
        storageDir = cmd.getOptionValue(CmdConst.STORAGE_PATH_L_NAME);
        if (StringUtils.isBlank(storageDir)) {
            storageDir = props.getStr(PropConst.STORAGE_DIR, ParamConst.STORAGE_DIR);
        }

        // get storage data directory
        storageDataDir = storageDir + File.separator + ParamConst.STORAGE_DATA_DIR;

        // get storage wal directory
        storageWalDir = storageDir + File.separator + ParamConst.STORAGE_WAL_DIR;

        // get storage wal archive directory
        storageWalArchiveDir = storageDir + File.separator + ParamConst.STORAGE_WAL_ARCHIVE_DIR;

        // get storage write synchronization mode
        storageWriteSyncMode = props.getStr(PropConst.STORAGE_WRITE_SYNC_MODE, ParamConst.STORAGE_WRITE_SYNC_MODE_PRIMARY_SYNC);

        // get storage wal mode
        storageWalMode = props.getStr(PropConst.STORAGE_WAL_MODE, ParamConst.STORAGE_WAL_MODE_LOG_ONLY);

        // get storage wal flush frequency
        storageWalFlushFrequencyMs = props.getInt(PropConst.STORAGE_WAL_FLUSH_FREQUENCY_MS, ParamConst.STORAGE_WAL_FLUSH_FREQUENCY_MS);

        // get log directory
        logDir = cmd.getOptionValue(CmdConst.LOG_PATH_L_NAME);
        if (StringUtils.isBlank(logDir)) {
            logDir = props.getStr(PropConst.LOG_DIR, ParamConst.LOG_DIR);
        }

        // get log level
        logLevel = LogUtil.level(cmd.getOptionValue(CmdConst.LOG_LEVEL_L_NAME));
        if (StringUtils.isBlank(logLevel)) {
            logLevel = LogUtil.level(props.getStr(PropConst.LOG_LEVEL, ParamConst.LOG_LEVEL_INFO));
        }

        // initialize storage directory
        mkdirs(cmd);
    }

    private static void port(CommandLine cmd) {
        // get mqtt listen port
        mqttPort = cmd.getOptionValue(CmdConst.MQTT_PORT_L_NAME);
        if (mqttPort == null || mqttPort <= 0) {
            mqttPort = props.getInt(PropConst.MQTT_PORT, ParamConst.MQTT_PORT);
        }

        // get mqtt ssl listen port
        mqttSslPort = cmd.getOptionValue(CmdConst.MQTT_SSL_PORT_L_NAME);
        if (mqttSslPort == null || mqttSslPort <= 0) {
            mqttSslPort = props.getInt(PropConst.MQTT_SSL_PORT, ParamConst.MQTT_SSL_PORT);
        }

        // get websocket listen port
        wsPort = cmd.getOptionValue(CmdConst.WS_PORT_L_NAME);
        if (wsPort == null || wsPort <= 0) {
            wsPort = props.getInt(PropConst.WS_PORT, ParamConst.WS_PORT);
        }

        // get websocket path
        wsPath = props.getStr(PropConst.WS_PATH, ParamConst.WS_PATH);

        // get websocket ssl listen port
        wssPort = cmd.getOptionValue(CmdConst.WSS_PORT_L_NAME);
        if (wssPort == null || wssPort <= 0) {
            wssPort = props.getInt(PropConst.WSS_PORT, ParamConst.WSS_PORT);
        }

        // get websocket ssl path
        wssPath = props.getStr(PropConst.WSS_PATH, ParamConst.WSS_PATH);

        // get http listen port
        httpPort = cmd.getOptionValue(CmdConst.HTTP_PORT_L_NAME);
        if (httpPort == null || httpPort <= 0) {
            httpPort = props.getInt(PropConst.HTTP_PORT, ParamConst.HTTP_PORT);
        }

        // get mqtt max connections
        mqttMaxConns = props.getInt(PropConst.MQTT_MAX_CONNECTIONS, ParamConst.MQTT_MAX_CONNECTIONS);

        // get mqtt ssl max connections
        mqttSslMaxConns = props.getInt(PropConst.MQTT_SSL_MAX_CONNECTIONS, ParamConst.MQTT_SSL_MAX_CONNECTIONS);

        // get websocket max connections
        wsMaxConns = props.getInt(PropConst.WS_MAX_CONNECTIONS, ParamConst.WS_MAX_CONNECTIONS);

        // get websocket ssl max connections
        wssMaxConns = props.getInt(PropConst.WSS_MAX_CONNECTIONS, ParamConst.WSS_MAX_CONNECTIONS);
    }

    private static void mqtt(CommandLine cmd) {
        // get client id max length
        mqttClientIdMaxLen = props.getInt(PropConst.MQTT_CLIENT_ID_MAX_LEN, ParamConst.MQTT_CLIENT_ID_MAX_LEN);

        // get client connect timeout
        mqttClientConnectTimeoutS = props.getInt(PropConst.MQTT_CLIENT_CONNECT_TIMEOUT_S, ParamConst.MQTT_CLIENT_CONNECT_TIMEOUT_S);

        // get client idle timeout
        mqttClientIdleTimeoutS = props.getInt(PropConst.MQTT_CLIENT_IDLE_TIMEOUT_S, ParamConst.MQTT_CLIENT_IDLE_TIMEOUT_S);

        // set packet max size(this property unit is KB), switch to bytes
        mqttPacketMaxSize = props.getInt(PropConst.MQTT_PACKET_MAX_SIZE, ParamConst.MQTT_PACKET_MAX_SIZE) * 1024;

        // set message max size
        mqttMessageMaxSize = mqttPacketMaxSize - ParamConst.MQTT_FIXED_HEADER_MIN_SIZE;

        // get mqtt retain message max capacity
        mqttRetainMaxCapacity = props.getInt(PropConst.MQTT_RETAIN_MAX_CAPACITY, ParamConst.MQTT_RETAIN_MAX_CAPACITY);

        // get retain message max size(this property unit is KB), switch to byte
        mqttRetainMaxPayload = props.getInt(PropConst.MQTT_RETAIN_MAX_PAYLOAD, ParamConst.MQTT_RETAIN_MAX_PAYLOAD) * 1024;

        // get retain message expiry interval
        mqttRetainExpiryIntervalS = props.getLong(PropConst.MQTT_RETAIN_EXPIRY_INTERVAL_S, ParamConst.MQTT_RETAIN_EXPIRY_INTERVAL_S);

        // get mqtt ssl enable
        mqttSslEnable = props.getBool(PropConst.MQTT_SSL_ENABLE, ParamConst.MQTT_SSL_ENABLE);

        // get mqtt ssl key path
        mqttSslKeyPath = props.getStr(PropConst.MQTT_SSL_KEY_PATH);

        // get mqtt ssl certificate path
        mqttSslCertPath = props.getStr(PropConst.MQTT_SSL_CERT_PATH);

        // get metrics enable
        mqttMetricsEnable = props.getBool(PropConst.MQTT_METRICS_ENABLE, ParamConst.MQTT_METRICS_ENABLE);
    }

    private static void session(CommandLine cmd) {
        // get session upgrade qos
        sessionUpgradeQos = props.getBool(PropConst.SESSION_UPGRADE_QOS, ParamConst.SESSION_UPGRADE_QOS);

        // get session publish qos2 message await 'PUBREL' max capacity
        sessionAwaitRelMaxCapacity = props.getInt(PropConst.SESSION_AWAIT_REL_MAX_CAPACITY, ParamConst.SESSION_AWAIT_REL_MAX_CAPACITY);

        // get session publish qos2 message await 'PUBREL' expiry interval
        sessionAwaitRelExpiryIntervalS = props.getLong(PropConst.SESSION_AWAIT_REL_EXPIRY_INTERVAL_S, ParamConst.SESSION_AWAIT_REL_EXPIRY_INTERVAL_S);

        // get mqtt session queue max capacity
        sessionQueueMaxCapacity = props.getInt(PropConst.SESSION_QUEUE_MAX_CAPACITY, ParamConst.SESSION_QUEUE_MAX_CAPACITY);

        // get mqtt session queue store the message(qos is 0)
        sessionQueueStoreQos0 = props.getBool(PropConst.SESSION_QUEUE_STORE_QOS0);

        // get mqtt session inflight max capacity
        sessionInflightMaxCapacity = props.getInt(PropConst.SESSION_INFLIGHT_MAX_CAPACITY, ParamConst.SESSION_INFLIGHT_MAX_CAPACITY);

        // get mqtt session inflight message expiry interval
        sessionInflightExpiryIntervalS = props.getLong(PropConst.SESSION_INFLIGHT_EXPIRY_INTERVAL_S, ParamConst.SESSION_INFLIGHT_EXPIRY_INTERVAL_S);

        // get mqtt session inflight message max retries
        sessionInflightMaxRetries = props.getInt(PropConst.SESSION_INFLIGHT_MAX_RETRIES, ParamConst.SESSION_INFLIGHT_MAX_RETRIES);
    }

    private static void auth(CommandLine cmd) {
        // initialize AES security instance
        aes = aes(cmd);

        // get allow anonymous
        authAllowAnonymous = props.getBool(PropConst.AUTH_ALLOW_ANONYMOUS, ParamConst.AUTH_ALLOW_ANONYMOUS);

        // get allow no matched in access control list
        authAclAllowNomatch = props.getBool(PropConst.AUTH_ACL_ALLOW_NOMATCH, ParamConst.AUTH_ACL_ALLOW_NOMATCH);

        // get authentication mode
        authMode = props.getStr(PropConst.AUTH_MODE, ParamConst.AUTH_MODE);

        // initialize redis authorization and authentication configurations
        redis(cmd);

        // initialize rdb authorization and authentication configurations
        rdb(cmd);

        // initialize mongodb authorization and authentication configurations
        mongodb(cmd);
    }

    private static void vertx(CommandLine cmd) {
        // get vertx multi-instances enable
        vertxMultiInstancesEnable = props.getBool(PropConst.VERTX_MULTI_INSTANCES_ENABLE, ParamConst.VERTX_MULTI_INSTANCES_ENABLE);

        // get vertx multi-instances
        vertxMultiInstances = props.getInt(PropConst.VERTX_MULTI_INSTANCES, ParamConst.VERTX_MULTI_INSTANCES);

        // get vertx worker pool size
        vertxWorkerPoolSize = props.getInt(PropConst.VERTX_WORKER_POOL_SIZE, ParamConst.VERTX_WORKER_POOL_SIZE);

        // enable/disable the file cache: system use the ".vertx" directory cache files
        // disable the cache in development mode/enable the cache in production mode
        vertxFileCachingEnabled = props.getBool(PropConst.VERTX_FILE_CACHING_ENABLED, ParamConst.VERTX_FILE_CACHING_ENABLED);

        // get http session timeout
        vertxHttpSessionTimeoutMs = props.getLong(PropConst.VERTX_HTTP_SESSION_TIMEOUT_MS, ParamConst.VERTX_HTTP_SESSION_TIMEOUT_MS);
    }

    private static void cluster(CommandLine cmd) {
        // get cluster mode configuration option
        clusterMode = cmd.getOptionValue(CmdConst.CLUSTER_MODE_L_NAME);
        if (StringUtils.isBlank(clusterMode)) {
            clusterMode = props.getStr(PropConst.CLUSTER_MODE, ParamConst.CLUSTER_MODE_STD);
        }

        // get cluster storage backups
        clusterStorageBackups = props.getInt(PropConst.CLUSTER_STORAGE_BACKUPS, ParamConst.CLUSTER_STORAGE_BACKUPS);

        // get cluster baseline topology rebalance time, when a persistence node(in
        // baseline topology) is left or failed, cluster baseline topology will
        // rebalance after this time
        clusterBltRebalanceTimeMs = props.getInt(PropConst.CLSUTER_BLT_REBALANCE_TIME_MS, ParamConst.CLSUTER_BLT_REBALANCE_TIME_MS);

        // get vmip addresses
        vmipAddresses = cmd.getOptionValue(CmdConst.VMIP_ADDRESSES_L_NAME);
        if (StringUtils.isBlank(vmipAddresses)) {
            vmipAddresses = props.getStr(PropConst.VMIP_ADDRESSES);
        }

        // get zookeeper connect string
        zkConnectString = cmd.getOptionValue(CmdConst.ZK_CONNECT_STRING_L_NAME);
        if (StringUtils.isBlank(zkConnectString)) {
            zkConnectString = props.getStr(PropConst.ZK_CONNECT_STRING);
        }

        // get zookeeper root path
        zkRootPath = cmd.getOptionValue(CmdConst.ZK_ROOT_PATH_L_NAME);
        if (StringUtils.isBlank(zkRootPath)) {
            zkRootPath = props.getStr(PropConst.ZK_ROOT_PATH, ParamConst.ZK_ROOT_PATH);
        }

        // get zookeeper join timeout
        zkJoinTimeoutMs = props.getLong(PropConst.ZK_JOIN_TIMEOUT_MS, ParamConst.ZK_JOIN_TIMEOUT_MS);

        // get zookeeper session timeout
        zkSessionTimeoutMs = props.getLong(PropConst.ZK_SESSION_TIMEOUT_MS, ParamConst.ZK_SESSION_TIMEOUT_MS);

        // get zookeeper client reconnect enable
        Boolean zkReconnectOptionValue = cmd.getOptionValue(CmdConst.ZK_RECONNECT_ENABLE_L_NAME);
        if (zkReconnectOptionValue != null) {
            zkReconnectEnable = zkReconnectOptionValue;
        } else {
            zkReconnectEnable = props.getBool(PropConst.ZK_RECONNECT_ENABLE, ParamConst.ZK_RECONNECT_ENABLE);
        }
    }

    private static void mkdirs(CommandLine cmd) {
        // check: storageDataDir is not blank
        if (StringUtils.isNotBlank(storageDataDir)) {
            // make dir
            DirUtil.mkdirs(storageDataDir);
        }

        // check: storageWalDir is not blank
        if (StringUtils.isNotBlank(storageWalDir)) {
            // make dir
            DirUtil.mkdirs(storageWalDir);
        }

        // check: storageWalArchiveDir is not blank
        if (StringUtils.isNotBlank(storageWalArchiveDir)) {
            // make dir
            DirUtil.mkdirs(storageWalArchiveDir);
        }

        // check: logDir is not blank
        if (StringUtils.isNotBlank(logDir)) {
            // make dir
            DirUtil.mkdirs(logDir);
        }
    }

    private static AES aes(CommandLine cmd) {
        Digester md5 = new Digester(DigestAlgorithm.MD5);

        // get authentication AES key
        String aesKey = props.getStr(PropConst.AUTH_AES_KEY, ParamConst.AUTH_AES_KEY);
        // md5 bytes
        byte[] raw = md5.digest(aesKey);
        // get key bytes
        byte[] key = SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), raw).getEncoded();

        return SecureUtil.aes(key);
    }

    private static void redis(CommandLine cmd) {
        // get authentication redis host
        authRedisHost = props.getStr(PropConst.AUTH_REDIS_HOST);

        // get authentication redis port
        authRedisPort = props.getInt(PropConst.AUTH_REDIS_PORT, ParamConst.AUTH_REDIS_PORT);

        // get authentication redis pass
        authRedisPass = props.getStr(PropConst.AUTH_REDIS_PASS);

        // get authentication redis select
        authRedisSelect = props.getInt(PropConst.AUTH_REDIS_SELECT, ParamConst.AUTH_REDIS_SELECT);

        // select > max
        if (authRedisSelect > ParamConst.AUTH_REDIS_SELECT_MAX) {
            authRedisSelect = authRedisSelect % ParamConst.AUTH_REDIS_SELECT_MAX;
        }

        // get authentication redis hget key prefix
        authRedisUserKeyPrefix = props.getStr(PropConst.AUTH_REDIS_USER_KEY_PREFIX, ParamConst.AUTH_REDIS_USER_KEY_PREFIX);

        // get authentication redis hget field
        authRedisPasswdField = props.getStr(PropConst.AUTH_REDIS_PASSWD_FIELD, ParamConst.AUTH_REDIS_PASSWD_FIELD);

        // get acl redis user key prefix
        authRedisAclUserKeyPrefix = props.getStr(PropConst.AUTH_REDIS_ACL_USER_KEY_PREFIX, ParamConst.AUTH_REDIS_ACL_USER_KEY_PREFIX);

        // get acl redis ip address key prefix
        authRedisAclIpAddrKeyPrefix = props.getStr(PropConst.AUTH_REDIS_ACL_IPADDR_KEY_PREFIX, ParamConst.AUTH_REDIS_ACL_IPADDR_KEY_PREFIX);

        // get acl redis client id key prefix
        authRedisAclClientKeyPrefix = props.getStr(PropConst.AUTH_REDIS_ACL_CLIENT_KEY_PREFIX, ParamConst.AUTH_REDIS_ACL_CLIENT_KEY_PREFIX);

        // get acl redis all key prefix
        authRedisAclAllKeyPrefix = props.getStr(PropConst.AUTH_REDIS_ACL_ALL_KEY_PREFIX, ParamConst.AUTH_REDIS_ACL_ALL_KEY_PREFIX);
    }

    private static void rdb(CommandLine cmd) {
        // get authentication MySQL/PostgreSQL host
        authRdbHost = props.getStr(PropConst.AUTH_RDB_HOST);

        // get authentication MySQL/PostgreSQL port
        if (ParamConst.AUTH_MODE_MYSQL.equalsIgnoreCase(authMode)) {
            authRdbPort = props.getInt(PropConst.AUTH_RDB_PORT, ParamConst.AUTH_MYSQL_PORT);
        }

        // get authentication MySQL/PostgreSQL username
        authRdbUsername = props.getStr(PropConst.AUTH_RDB_USERNAME);

        // get authentication MySQL/PostgreSQL password
        authRdbPassword = props.getStr(PropConst.AUTH_RDB_PASSWORD);

        // get authentication MySQL/PostgreSQL database
        authRdbDatabase = props.getStr(PropConst.AUTH_RDB_DATABASE);

        // get authentication MySQL/PostgreSQL charset
        authRdbCharset = props.getStr(PropConst.AUTH_RDB_CHARSET, ParamConst.AUTH_RDB_CHARSET);

        // get authentication MySQL/PostgreSQL max pool size
        authRdbMaxPoolSize = props.getInt(PropConst.AUTH_RDB_MAX_POOL_SIZE, ParamConst.AUTH_RDB_MAX_POOL_SIZE);

        // get authentication MySQL/PostgreSQL query timeout
        authRdbQueryTimeoutMs = props.getInt(PropConst.AUTH_RDB_QUERY_TIMEOUT_MS, ParamConst.AUTH_RDB_QUERY_TIMEOUT_MS);
    }

    private static void mongodb(CommandLine cmd) {
        // get authentication MongoDB host
        authMongoHost = props.getStr(PropConst.AUTH_MONGO_HOST);

        // get authentication MongoDB port
        authMongoPort = props.getInt(PropConst.AUTH_MONGO_PORT, ParamConst.AUTH_MONGO_PORT);

        // get authentication MongoDB database name
        authMongoDbName = props.getStr(PropConst.AUTH_MONGO_DB_NAME);

        // get authentication MongoDB username
        authMongoUsername = props.getStr(PropConst.AUTH_MONGO_USERNAME);

        // get authentication MongoDB password
        authMongoPassword = props.getStr(PropConst.AUTH_MONGO_PASSWORD);

        // get authentication MongoDB authentication source
        authMongoAuthSource = props.getStr(PropConst.AUTH_MONGO_AUTH_SOURCE);

        // get authentication MongoDB authentication mechanism
        authMongoAuthMechanism = props.getStr(PropConst.AUTH_MONGO_AUTH_MECHANISM, ParamConst.AUTH_MONGO_AUTH_MECHANISM);

        // get authentication MongoDB max pool size
        authMongoMaxPoolSize = props.getInt(PropConst.AUTH_MONGO_MAX_POOL_SIZE, ParamConst.AUTH_MONGO_MAX_POOL_SIZE);

        // get authentication MongoDB min pool size
        authMongoMinPoolSize = props.getInt(PropConst.AUTH_MONGO_MIN_POOL_SIZE, ParamConst.AUTH_MONGO_MIN_POOL_SIZE);

        // get authentication MongoDB max idle time
        authMongoMaxIdleTimeMs = props.getLong(PropConst.AUTH_MONGO_MAX_IDLE_TIME_MS, ParamConst.AUTH_MONGO_MAX_IDLE_TIME_MS);

        // get authentication MongoDB max life time
        authMongoMaxLifeTimeMs = props.getLong(PropConst.AUTH_MONGO_MAX_LIFE_TIME_MS, ParamConst.AUTH_MONGO_MAX_LIFE_TIME_MS);

        // get authentication MongoDB wait queue multiple
        authMongoWaitQueueMultiple = props.getInt(PropConst.AUTH_MONGO_WAIT_QUEUE_MULTIPLE, ParamConst.AUTH_MONGO_WAIT_QUEUE_MULTIPLE);

        // get authentication MongoDB wait queue timeout
        authMongoWaitQueueTimeoutMs = props.getLong(PropConst.AUTH_MONGO_WAIT_QUEUE_TIMEOUT_MS, ParamConst.AUTH_MONGO_WAIT_QUEUE_TIMEOUT_MS);

        // get authentication MongoDB maintenance frequency
        authMongoMaintenanceFrequencyMs = props.getLong(PropConst.AUTH_MONGO_MAINTENANCE_FREQUENCY_MS, ParamConst.AUTH_MONGO_MAINTENANCE_FREQUENCY_MS);

        // get authentication MongoDB maintenance initial delay
        authMongoMaintenanceInitialDelayMs = props.getLong(PropConst.AUTH_MONGO_MAINTENANCE_INITIAL_DELAY_MS, ParamConst.AUTH_MONGO_MAINTENANCE_INITIAL_DELAY_MS);

        // get authentication MongoDB connect timeout
        authMongoConnectTimeoutMs = props.getInt(PropConst.AUTH_MONGO_CONNECT_TIMEOUT_MS, ParamConst.AUTH_MONGO_CONNECT_TIMEOUT_MS);

        // get authentication MongoDB socket timeout
        authMongoSocketTimeoutMs = props.getInt(PropConst.AUTH_MONGO_SOCKET_TIMEOUT_MS, ParamConst.AUTH_MONGO_SOCKET_TIMEOUT_MS);

        // get MongoDB user collection name
        authMongoUser = props.getStr(PropConst.AUTH_MONGO_USER, ParamConst.AUTH_MONGO_USER);

        // get MongoDB user collection username field
        authMongoUserUsernameField = props.getStr(PropConst.AUTH_MONGO_USER_USERNAME_FIELD, ParamConst.AUTH_MONGO_USER_USERNAME_FIELD);

        // get MongoDB user collection password field
        authMongoUserPasswordField = props.getStr(PropConst.AUTH_MONGO_USER_PASSWORD_FIELD, ParamConst.AUTH_MONGO_USER_PASSWORD_FIELD);

        // get MongoDB acl collection name
        authMongoAcl = props.getStr(PropConst.AUTH_MONGO_ACL, ParamConst.AUTH_MONGO_ACL);

        // get MongoDB acl collection target field
        authMongoAclTargetField = props.getStr(PropConst.AUTH_MONGO_ACL_TARGET_FIELD, ParamConst.AUTH_MONGO_ACL_TARGET_FIELD);

        // get MongoDB acl collection type field
        authMongoAclTypeField = props.getStr(PropConst.AUTH_MONGO_ACL_TYPE_FIELD, ParamConst.AUTH_MONGO_ACL_TYPE_FIELD);

        // get MongoDB acl collection seq field
        authMongoAclSeqField = props.getStr(PropConst.AUTH_MONGO_ACL_SEQ_FIELD, ParamConst.AUTH_MONGO_ACL_SEQ_FIELD);

        // get MongoDB acl collection topics field
        authMongoAclTopicsField = props.getStr(PropConst.AUTH_MONGO_ACL_TOPICS_FIELD, ParamConst.AUTH_MONGO_ACL_TOPICS_FIELD);

        // get MongoDB acl collection topic field
        authMongoAclTopicField = props.getStr(PropConst.AUTH_MONGO_ACL_TOPIC_FIELD, ParamConst.AUTH_MONGO_ACL_TOPIC_FIELD);

        // get MongoDB acl collection authority field
        authMongoAclAuthorityField = props.getStr(PropConst.AUTH_MONGO_ACL_AUTHORITY_FIELD, ParamConst.AUTH_MONGO_ACL_AUTHORITY_FIELD);
    }

}
