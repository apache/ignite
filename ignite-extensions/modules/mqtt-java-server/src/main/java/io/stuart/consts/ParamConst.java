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

public interface ParamConst {

    static final String CFG_PATH = "cfg.properties";

    static final String INSTANCE_ID = "stuart";

    static final String INSTANCE_LISTEN_ADDR = "0.0.0.0";

    static final long INSTANCE_METRICS_PERIOD_MS = 60000;

    static final String STORAGE_DIR = "./storage";

    static final String STORAGE_DATA_DIR = "data";

    static final String STORAGE_WAL_DIR = "wal";

    static final String STORAGE_WAL_ARCHIVE_DIR = "archive";

    static final String STORAGE_WRITE_SYNC_MODE_PRIMARY_SYNC = "primary_sync";

    static final String STORAGE_WRITE_SYNC_MODE_FULL_SYNC = "full_sync";

    // TODO CacheWriteSynchronizationMode.FULL_ASYNC: Some scenarios are not
    // applicable, optimized separately for different caches
    // static final String STORAGE_WRITE_SYNC_MODE_FULL_ASYNC = "full_async";

    static final String STORAGE_WAL_MODE_FSYNC = "fsync";

    static final String STORAGE_WAL_MODE_LOG_ONLY = "log_only";

    static final String STORAGE_WAL_MODE_BACKGROUND = "background";

    static final int STORAGE_WAL_FLUSH_FREQUENCY_MS = 2000;

    static final String LOG_DIR = "./log";

    static final String LOG_LEVEL_ALL = "ALL";

    static final String LOG_LEVEL_DEBUG = "DEBUG";

    static final String LOG_LEVEL_ERROR = "ERROR";

    static final String LOG_LEVEL_FATAL = "FATAL";

    static final String LOG_LEVEL_INFO = "INFO";

    static final String LOG_LEVEL_OFF = "OFF";

    static final String LOG_LEVEL_TRACE = "TRACE";

    static final String LOG_LEVEL_WARN = "WARN";

    static final int MQTT_PORT = 1883;

    static final int MQTT_SSL_PORT = 8883;

    static final int WS_PORT = 8080;

    static final String WS_PATH = "/mqtt";

    static final int WSS_PORT = 8083;

    static final String WSS_PATH = "/mqtt";

    static final int HTTP_PORT = 18083;

    static final int MQTT_MAX_CONNECTIONS = 102400;

    static final int MQTT_SSL_MAX_CONNECTIONS = 1024;

    static final int WS_MAX_CONNECTIONS = 64;

    static final int WSS_MAX_CONNECTIONS = 64;

    static final int MQTT_CLIENT_ID_MAX_LEN = 1024;

    static final int MQTT_CLIENT_CONNECT_TIMEOUT_S = 90;

    static final int MQTT_CLIENT_IDLE_TIMEOUT_S = 30;

    static final int MQTT_PACKET_MAX_SIZE = 64;

    static final int MQTT_FIXED_HEADER_MIN_SIZE = 2;

    static final int MQTT_RETAIN_MAX_CAPACITY = 1000000;

    static final int MQTT_RETAIN_MAX_PAYLOAD = 64;

    static final long MQTT_RETAIN_EXPIRY_INTERVAL_S = 0;

    static final boolean MQTT_SSL_ENABLE = false;

    static final boolean MQTT_METRICS_ENABLE = false;

    static final boolean SESSION_UPGRADE_QOS = false;

    static final int SESSION_AWAIT_REL_MAX_CAPACITY = 100;

    static final long SESSION_AWAIT_REL_EXPIRY_INTERVAL_S = 20;

    static final int SESSION_QUEUE_MAX_CAPACITY = 1000;

    static final boolean SESSION_QUEUE_STORE_QOS0 = false;

    static final int SESSION_INFLIGHT_MAX_CAPACITY = 20;

    static final long SESSION_INFLIGHT_EXPIRY_INTERVAL_S = 20;

    static final int SESSION_INFLIGHT_MAX_RETRIES = 3;

    static final String AUTH_AES_KEY = "stuart_secret_key";

    static final boolean AUTH_ALLOW_ANONYMOUS = true;

    static final boolean AUTH_ACL_ALLOW_NOMATCH = true;

    static final String AUTH_MODE = "local";

    static final String AUTH_MODE_LOCAL = "local";

    static final String AUTH_MODE_REDIS = "redis";

    static final String AUTH_MODE_MYSQL = "jdbc";

    static final String AUTH_MODE_MONGO = "mongo";

    static final int AUTH_REDIS_PORT = 6379;

    static final int AUTH_REDIS_SELECT = 0;

    static final int AUTH_REDIS_SELECT_MAX = 15;

    static final String AUTH_REDIS_USER_KEY_PREFIX = "stuart:auth_user:";

    static final String AUTH_REDIS_PASSWD_FIELD = "password";

    static final String AUTH_REDIS_ACL_USER_KEY_PREFIX = "stuart:acl_user:";

    static final String AUTH_REDIS_ACL_IPADDR_KEY_PREFIX = "stuart:acl_ipaddr:";

    static final String AUTH_REDIS_ACL_CLIENT_KEY_PREFIX = "stuart:acl_client:";

    static final String AUTH_REDIS_ACL_ALL_KEY_PREFIX = "stuart:acl_all:";

    static final int AUTH_MYSQL_PORT = 3306;

    static final int AUTH_POSTGRESQL_PORT = 5432;

    static final String AUTH_RDB_CHARSET = "UTF-8";

    static final int AUTH_RDB_MAX_POOL_SIZE = 10;

    static final int AUTH_RDB_QUERY_TIMEOUT_MS = 10000;

    static final int AUTH_MONGO_PORT = 27017;

    static final String AUTH_MONGO_AUTH_MECHANISM = "SCRAM-SHA-1";

    static final int AUTH_MONGO_MAX_POOL_SIZE = 100;

    static final int AUTH_MONGO_MIN_POOL_SIZE = 100;

    static final long AUTH_MONGO_MAX_IDLE_TIME_MS = 0;

    static final long AUTH_MONGO_MAX_LIFE_TIME_MS = 0;

    static final int AUTH_MONGO_WAIT_QUEUE_MULTIPLE = 500;

    static final long AUTH_MONGO_WAIT_QUEUE_TIMEOUT_MS = 120000;

    static final long AUTH_MONGO_MAINTENANCE_FREQUENCY_MS = 0;

    static final long AUTH_MONGO_MAINTENANCE_INITIAL_DELAY_MS = 0;

    static final int AUTH_MONGO_CONNECT_TIMEOUT_MS = 10000;

    static final int AUTH_MONGO_SOCKET_TIMEOUT_MS = 0;

    static final String AUTH_MONGO_USER = "wc_accounts";

    static final String AUTH_MONGO_USER_USERNAME_FIELD = "username";

    static final String AUTH_MONGO_USER_PASSWORD_FIELD = "password";

    static final String AUTH_MONGO_ACL = "stuart_acl";

    static final String AUTH_MONGO_ACL_TARGET_FIELD = "target";

    static final String AUTH_MONGO_ACL_TYPE_FIELD = "type";

    static final String AUTH_MONGO_ACL_SEQ_FIELD = "seq";

    static final String AUTH_MONGO_ACL_TOPICS_FIELD = "topics";

    static final String AUTH_MONGO_ACL_TOPIC_FIELD = "topic";

    static final String AUTH_MONGO_ACL_AUTHORITY_FIELD = "authority";

    static final String AUTH_MONGO_QUERY_FIELD_SEPARATOR = ".";

    static final boolean VERTX_MULTI_INSTANCES_ENABLE = false;

    static final int VERTX_MULTI_INSTANCES = 1;

    static final int VERTX_WORKER_POOL_SIZE = 2;

    static final boolean VERTX_FILE_CACHING_ENABLED = true;

    static final long VERTX_HTTP_SESSION_TIMEOUT_MS = 60 * 60 * 1000;

    static final String CLUSTER_MODE_STD = "standalone";

    static final String CLUSTER_MODE_VMIP = "vmip";

    static final String CLUSTER_MODE_ZK = "zookeeper";

    static final int CLUSTER_STORAGE_BACKUPS = 2;

    static final int CLSUTER_BLT_REBALANCE_TIME_MS = 5 * 60 * 1000;

    static final String STD_LOCAL_IP = "127.0.0.1";

    static final int STD_LOCAL_PORT = 47500;

    static final String ZK_ROOT_PATH = "/stuart";

    static final long ZK_JOIN_TIMEOUT_MS = 10000;

    static final long ZK_SESSION_TIMEOUT_MS = 20000;

    static final boolean ZK_RECONNECT_ENABLE = true;

}
