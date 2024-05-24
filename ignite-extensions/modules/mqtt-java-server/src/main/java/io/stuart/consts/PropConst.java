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

public interface PropConst {

    static final String INSTANCE_ID = "instance.id";

    static final String INSTANCE_LISTEN_ADDR = "instance.listen-address";

    static final String INSTANCE_METRICS_PERIOD_MS = "instance.metrics-period-ms";

    static final String STORAGE_DIR = "instance.storage.dir";

    static final String STORAGE_WRITE_SYNC_MODE = "instance.storage-write-sync-mode";

    static final String STORAGE_WAL_MODE = "instance.storage.wal-mode";

    static final String STORAGE_WAL_FLUSH_FREQUENCY_MS = "instance.storage.wal-flush-frequency-ms";

    static final String LOG_DIR = "instance.log.dir";

    static final String LOG_LEVEL = "instance.log.level";

    static final String MQTT_PORT = "mqtt.port";

    static final String MQTT_SSL_PORT = "mqtt.ssl-port";

    static final String WS_PORT = "websocket.port";

    static final String WS_PATH = "websocket.path";

    static final String WSS_PORT = "websocket.ssl-port";

    static final String WSS_PATH = "websocket.ssl-path";

    static final String HTTP_PORT = "http.port";

    static final String MQTT_MAX_CONNECTIONS = "mqtt.max-connections";

    static final String MQTT_SSL_MAX_CONNECTIONS = "mqtt.ssl-max-connections";

    static final String WS_MAX_CONNECTIONS = "websocket.max-connections";

    static final String WSS_MAX_CONNECTIONS = "websocket.ssl-max-connections";

    static final String MQTT_CLIENT_ID_MAX_LEN = "mqtt.client.max-len";

    static final String MQTT_CLIENT_CONNECT_TIMEOUT_S = "mqtt.client.connect-timeout-s";

    static final String MQTT_CLIENT_IDLE_TIMEOUT_S = "mqtt.client.idle-timeout-s";

    static final String MQTT_PACKET_MAX_SIZE = "mqtt.packet.max-size-kb";

    static final String MQTT_RETAIN_MAX_CAPACITY = "mqtt.retain.max-capacity";

    static final String MQTT_RETAIN_MAX_PAYLOAD = "mqtt.retain.max-payload-kb";

    static final String MQTT_RETAIN_EXPIRY_INTERVAL_S = "mqtt.retain.expiry-interval-s";

    static final String MQTT_SSL_ENABLE = "mqtt.ssl-enable";

    static final String MQTT_SSL_KEY_PATH = "mqtt.ssl-key-path";

    static final String MQTT_SSL_CERT_PATH = "mqtt.ssl-cert-path";

    static final String MQTT_METRICS_ENABLE = "mqtt.metrics-enable";

    static final String SESSION_UPGRADE_QOS = "session.upgrade-qos";

    static final String SESSION_AWAIT_REL_MAX_CAPACITY = "session.await-rel.max-capacity";

    static final String SESSION_AWAIT_REL_EXPIRY_INTERVAL_S = "session.await-rel.expiry-interval-s";

    static final String SESSION_QUEUE_MAX_CAPACITY = "session.queue.max-capacity";

    static final String SESSION_QUEUE_STORE_QOS0 = "session.queue.store-qos0";

    static final String SESSION_INFLIGHT_MAX_CAPACITY = "session.inflight.max-capacity";

    static final String SESSION_INFLIGHT_EXPIRY_INTERVAL_S = "session.inflight.expiry-interval-s";

    static final String SESSION_INFLIGHT_MAX_RETRIES = "session.inflight.max-retries";

    static final String AUTH_AES_KEY = "auth.aes-key";

    static final String AUTH_ALLOW_ANONYMOUS = "auth.allow-anonymous";

    static final String AUTH_ACL_ALLOW_NOMATCH = "auth.acl-allow-nomatch";

    static final String AUTH_MODE = "auth.mode";

    static final String AUTH_REDIS_HOST = "auth.redis.host";

    static final String AUTH_REDIS_PORT = "auth.redis.port";

    static final String AUTH_REDIS_PASS = "auth.redis.pass";

    static final String AUTH_REDIS_SELECT = "auth.redis.select";

    static final String AUTH_REDIS_USER_KEY_PREFIX = "auth.redis.user-key-prefix";

    static final String AUTH_REDIS_PASSWD_FIELD = "auth.redis.passwd-field";

    static final String AUTH_REDIS_ACL_USER_KEY_PREFIX = "auth.redis.acl.user-key-prefix";

    static final String AUTH_REDIS_ACL_IPADDR_KEY_PREFIX = "auth.redis.acl.ipaddr-key-prefix";

    static final String AUTH_REDIS_ACL_CLIENT_KEY_PREFIX = "auth.redis.acl.client-key-prefix";

    static final String AUTH_REDIS_ACL_ALL_KEY_PREFIX = "auth.redis.acl.all-key-prefix";

    static final String AUTH_RDB_HOST = "auth.rdb.host";

    static final String AUTH_RDB_PORT = "auth.rdb.port";

    static final String AUTH_RDB_USERNAME = "auth.rdb.username";

    static final String AUTH_RDB_PASSWORD = "auth.rdb.password";

    static final String AUTH_RDB_DATABASE = "auth.rdb.database";

    static final String AUTH_RDB_CHARSET = "auth.rdb.charset";

    static final String AUTH_RDB_MAX_POOL_SIZE = "auth.rdb.max-pool-size";

    static final String AUTH_RDB_QUERY_TIMEOUT_MS = "auth.rdb.query-timeout-ms";

    static final String AUTH_MONGO_HOST = "auth.mongo.host";

    static final String AUTH_MONGO_PORT = "auth.mongo.port";

    static final String AUTH_MONGO_DB_NAME = "auth.mongo.db-name";

    static final String AUTH_MONGO_USERNAME = "auth.mongo.username";

    static final String AUTH_MONGO_PASSWORD = "auth.mongo.password";

    static final String AUTH_MONGO_AUTH_SOURCE = "auth.mongo.auth-source";

    static final String AUTH_MONGO_AUTH_MECHANISM = "auth.mongo.auth-mechanism";

    static final String AUTH_MONGO_MAX_POOL_SIZE = "auth.mongo.max-pool-size";

    static final String AUTH_MONGO_MIN_POOL_SIZE = "auth.mongo.min-pool-size";

    static final String AUTH_MONGO_MAX_IDLE_TIME_MS = "auth.mongo.max-idle-time-ms";

    static final String AUTH_MONGO_MAX_LIFE_TIME_MS = "auth.mongo.max-life-time-ms";

    static final String AUTH_MONGO_WAIT_QUEUE_MULTIPLE = "auth.mongo.wait-queue-multiple";

    static final String AUTH_MONGO_WAIT_QUEUE_TIMEOUT_MS = "auth.mongo.wait-queue-timeout-ms";

    static final String AUTH_MONGO_MAINTENANCE_FREQUENCY_MS = "auth.mongo.maintenance-frequency-ms";

    static final String AUTH_MONGO_MAINTENANCE_INITIAL_DELAY_MS = "auth.mongo.maintenance-initial-delay-ms";

    static final String AUTH_MONGO_CONNECT_TIMEOUT_MS = "auth.mongo.connect-timeout-ms";

    static final String AUTH_MONGO_SOCKET_TIMEOUT_MS = "auth.mongo.socket-timeout-ms";

    static final String AUTH_MONGO_USER = "auth.mongo.user";

    static final String AUTH_MONGO_USER_USERNAME_FIELD = "auth.mongo.user.username-field";

    static final String AUTH_MONGO_USER_PASSWORD_FIELD = "auth.mongo.user.password-field";

    static final String AUTH_MONGO_ACL = "auth.mongo.acl";

    static final String AUTH_MONGO_ACL_TARGET_FIELD = "auth.mongo.acl.target-field";

    static final String AUTH_MONGO_ACL_TYPE_FIELD = "auth.mongo.acl.type-field";

    static final String AUTH_MONGO_ACL_SEQ_FIELD = "auth.mongo.acl.seq-field";

    static final String AUTH_MONGO_ACL_TOPICS_FIELD = "auth.mongo.acl.topics-field";

    static final String AUTH_MONGO_ACL_TOPIC_FIELD = "auth.mongo.acl.topic-field";

    static final String AUTH_MONGO_ACL_AUTHORITY_FIELD = "auth.mongo.acl.authority-field";

    static final String VERTX_MULTI_INSTANCES_ENABLE = "vertx.multi-instances-enable";

    static final String VERTX_MULTI_INSTANCES = "vertx.multi-instances";

    static final String VERTX_WORKER_POOL_SIZE = "vertx.worker-pool-size";

    static final String VERTX_FILE_CACHING_ENABLED = "vertx.file-caching.enabled";

    static final String VERTX_HTTP_SESSION_TIMEOUT_MS = "vertx.http.session-timeout-ms";

    static final String CLUSTER_MODE = "cluster.mode";

    static final String CLUSTER_STORAGE_BACKUPS = "cluster.storage-backups";

    static final String CLSUTER_BLT_REBALANCE_TIME_MS = "cluster.blt-rebalance-time-ms";

    static final String VMIP_ADDRESSES = "vmip.addresses";

    static final String ZK_CONNECT_STRING = "zookeeper.connect-string";

    static final String ZK_ROOT_PATH = "zookeeper.root-path";

    static final String ZK_JOIN_TIMEOUT_MS = "zookeeper.join-timeout-ms";

    static final String ZK_SESSION_TIMEOUT_MS = "zookeeper.session-timeout-ms";

    static final String ZK_RECONNECT_ENABLE = "zookeeper.reconnect.enable";

}
