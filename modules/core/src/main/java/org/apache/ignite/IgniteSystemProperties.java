/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.io.Serializable;
import java.lang.management.RuntimeMXBean;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.HostnameVerifier;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.util.GridLogThrottle;
import org.apache.ignite.stream.StreamTransformer;
import org.jetbrains.annotations.Nullable;

/**
 * Contains constants for all system properties and environmental variables in Ignite.
 * These properties and variables can be used to affect the behavior of Ignite.
 */
public final class IgniteSystemProperties {
    /**
     * If this system property is present the Ignite will include grid name into verbose log.
     *
     * @deprecated Use {@link #IGNITE_LOG_INSTANCE_NAME}.
     */
    @Deprecated
    public static final String IGNITE_LOG_GRID_NAME = "IGNITE_LOG_GRID_NAME";

    /**
     * If this system property is present the Ignite will include instance name into verbose log.
     */
    public static final String IGNITE_LOG_INSTANCE_NAME = "IGNITE_LOG_INSTANCE_NAME";

    /**
     * This property is used internally to pass an exit code to loader when
     * Ignite instance is being restarted.
     */
    public static final String IGNITE_RESTART_CODE = "IGNITE_RESTART_CODE";

    /**
     * Presence of this system property with value {@code true} will make the grid
     * node start as a daemon node. Node that this system property will override
     * {@link org.apache.ignite.configuration.IgniteConfiguration#isDaemon()} configuration.
     */
    public static final String IGNITE_DAEMON = "IGNITE_DAEMON";

    /** Defines Ignite installation folder. */
    public static final String IGNITE_HOME = "IGNITE_HOME";

    /** If this system property is set to {@code true} - no shutdown hook will be set. */
    public static final String IGNITE_NO_SHUTDOWN_HOOK = "IGNITE_NO_SHUTDOWN_HOOK";

    /**
     * Name of the system property to disable requirement for proper node ordering
     * by discovery SPI. Use with care, as proper node ordering is required for
     * cache consistency. If set to {@code true}, then any discovery SPI can be used
     * with distributed cache, otherwise, only discovery SPIs that have annotation
     * {@link org.apache.ignite.spi.discovery.DiscoverySpiOrderSupport @GridDiscoverySpiOrderSupport(true)} will
     * be allowed.
     */
    public static final String IGNITE_NO_DISCO_ORDER = "IGNITE_NO_DISCO_ORDER";

    /** Defines reconnect delay in milliseconds for client node that was failed forcible. */
    public static final String IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY = "IGNITE_DISCO_FAILED_CLIENT_RECONNECT_DELAY";

    /**
     * If this system property is set to {@code false} - no checks for new versions will
     * be performed by Ignite. By default, Ignite periodically checks for the new
     * version and prints out the message into the log if new version of Ignite is
     * available for download.
     */
    public static final String IGNITE_UPDATE_NOTIFIER = "IGNITE_UPDATE_NOTIFIER";

    /**
     * This system property defines interval in milliseconds in which Ignite will check
     * thread pool state for starvation. Zero value will disable this checker.
     */
    public static final String IGNITE_STARVATION_CHECK_INTERVAL = "IGNITE_STARVATION_CHECK_INTERVAL";

    /**
     * If this system property is present (any value) - no ASCII logo will
     * be printed.
     */
    public static final String IGNITE_NO_ASCII = "IGNITE_NO_ASCII";

    /**
     * This property allows to override Jetty host for REST processor.
     */
    public static final String IGNITE_JETTY_HOST = "IGNITE_JETTY_HOST";

    /**
     * This property allows to override Jetty local port for REST processor.
     */
    public static final String IGNITE_JETTY_PORT = "IGNITE_JETTY_PORT";

    /**
     * This property does not allow Ignite to override Jetty log configuration for REST processor.
     */
    public static final String IGNITE_JETTY_LOG_NO_OVERRIDE = "IGNITE_JETTY_LOG_NO_OVERRIDE";

    /** This property allow rewriting default ({@code 30}) REST session expire time (in seconds). */
    public static final String IGNITE_REST_SESSION_TIMEOUT = "IGNITE_REST_SESSION_TIMEOUT";

    /** This property allow rewriting default ({@code 300}) REST session security token expire time (in seconds). */
    public static final String IGNITE_REST_SECURITY_TOKEN_TIMEOUT = "IGNITE_REST_SECURITY_TOKEN_TIMEOUT";

    /**
     * This property allows to override maximum count of task results stored on one node
     * in REST processor.
     */
    public static final String IGNITE_REST_MAX_TASK_RESULTS = "IGNITE_REST_MAX_TASK_RESULTS";

    /**
     * This property allows to override default behavior that rest processor
     * doesn't start on client node. If set {@code true} than rest processor will be started on client node.
     */
    public static final String IGNITE_REST_START_ON_CLIENT = "IGNITE_REST_START_ON_CLIENT";

    /**
     * This property defines the maximum number of attempts to remap near get to the same
     * primary node. Remapping may be needed when topology is changed concurrently with
     * get operation.
     */
    public static final String IGNITE_NEAR_GET_MAX_REMAPS = "IGNITE_NEAR_GET_MAX_REMAPS";

    /**
     * Set to either {@code true} or {@code false} to enable or disable quiet mode
     * of Ignite. In quiet mode, only warning and errors are printed into the log
     * additionally to a shortened version of standard output on the start.
     * <p>
     * Note that if you use <tt>ignite.{sh|bat}</tt> scripts to start Ignite they
     * start by default in quiet mode. You can supply <tt>-v</tt> flag to override it.
     */
    public static final String IGNITE_QUIET = "IGNITE_QUIET";

    /**
     * Setting this option to {@code true} will enable troubleshooting logger.
     * Troubleshooting logger makes logging more verbose without enabling debug mode
     * to provide more detailed logs without performance penalty.
     */
    public static final String IGNITE_TROUBLESHOOTING_LOGGER = "IGNITE_TROUBLESHOOTING_LOGGER";

    /**
     * Setting to {@code true} enables writing sensitive information in {@code toString()} output.
     */
    public static final String IGNITE_TO_STRING_INCLUDE_SENSITIVE = "IGNITE_TO_STRING_INCLUDE_SENSITIVE";

    /** Maximum length for {@code toString()} result. */
    public static final String IGNITE_TO_STRING_MAX_LENGTH = "IGNITE_TO_STRING_MAX_LENGTH";

    /**
     * Limit collection (map, array) elements number to output.
     */
    public static final String IGNITE_TO_STRING_COLLECTION_LIMIT = "IGNITE_TO_STRING_COLLECTION_LIMIT";

    /**
     * If this property is set to {@code true} (default) and Ignite is launched
     * in verbose mode (see {@link #IGNITE_QUIET}) and no console appenders can be found
     * in configuration, then default console appender will be added.
     * Set this property to {@code false} if no appenders should be added.
     */
    public static final String IGNITE_CONSOLE_APPENDER = "IGNITE_CONSOLE_APPENDER";

    /** Maximum size for exchange history. Default value is {@code 1000}.*/
    public static final String IGNITE_EXCHANGE_HISTORY_SIZE = "IGNITE_EXCHANGE_HISTORY_SIZE";

    /** */
    public static final String IGNITE_EXCHANGE_MERGE_DELAY = "IGNITE_EXCHANGE_MERGE_DELAY";

    /**
     * Name of the system property defining name of command line program.
     */
    public static final String IGNITE_PROG_NAME = "IGNITE_PROG_NAME";

    /**
     * Name of the system property defining success file name. This file
     * is used with auto-restarting functionality when Ignite is started
     * by supplied <tt>ignite.{bat|sh}</tt> scripts.
     */
    public static final String IGNITE_SUCCESS_FILE = "IGNITE_SUCCESS_FILE";

    /**
     * The system property sets a system-wide local IP address or hostname to be used by Ignite networking components.
     * Once provided, the property overrides all the default local binding settings for Ignite nodes.
     * <p>
     * Note, that the address can also be changed via
     * {@link org.apache.ignite.configuration.IgniteConfiguration#setLocalHost(String)} method.
     * However, this system property has bigger priority and overrides the settings set via
     * {@link org.apache.ignite.configuration.IgniteConfiguration}.
     */
    public static final String IGNITE_LOCAL_HOST = "IGNITE_LOCAL_HOST";

    /**
     * System property to override deployment mode configuration parameter.
     * Valid values for property are: PRIVATE, ISOLATED, SHARED or CONTINUOUS.
     *
     * @see org.apache.ignite.configuration.DeploymentMode
     * @see org.apache.ignite.configuration.IgniteConfiguration#getDeploymentMode()
     */
    public static final String IGNITE_DEP_MODE_OVERRIDE = "IGNITE_DEPLOYMENT_MODE_OVERRIDE";

    /**
     * Property controlling size of buffer holding completed transaction versions. Such buffer
     * is used to detect duplicate transaction and has a default value of {@code 102400}. In
     * most cases this value is large enough and does not need to be changed.
     */
    public static final String IGNITE_MAX_COMPLETED_TX_COUNT = "IGNITE_MAX_COMPLETED_TX_COUNT";

    /**
     * Transactions that take more time, than value of this property, will be output to log
     * with warning level. {@code 0} (default value) disables warning on slow transactions.
     */
    public static final String IGNITE_SLOW_TX_WARN_TIMEOUT = "IGNITE_SLOW_TX_WARN_TIMEOUT";

    /**
     * Timeout after which all uncompleted transactions originated by left node will be
     * salvaged (i.e. invalidated and committed).
     */
    public static final String IGNITE_TX_SALVAGE_TIMEOUT = "IGNITE_TX_SALVAGE_TIMEOUT";

    /**
     * Specifies maximum number of iterations for deadlock detection procedure.
     * If value of this property is less then or equal to zero then deadlock detection will be disabled.
     */
    public static final String IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS = "IGNITE_TX_DEADLOCK_DETECTION_MAX_ITERS";

    /**
     * Specifies timeout for deadlock detection procedure.
     */
    public static final String IGNITE_TX_DEADLOCK_DETECTION_TIMEOUT = "IGNITE_TX_DEADLOCK_DETECTION_TIMEOUT";

    /**
     * System property to override multicast group taken from configuration.
     * Used for testing purposes.
     */
    public static final String IGNITE_OVERRIDE_MCAST_GRP = "IGNITE_OVERRIDE_MCAST_GRP";

    /**
     * System property to override default reflection cache size. Default value is {@code 128}.
     */
    public static final String IGNITE_REFLECTION_CACHE_SIZE = "IGNITE_REFLECTION_CACHE_SIZE";

    /**
     * System property to override default job processor maps sizes for finished jobs and
     * cancellation requests. Default value is {@code 10240}.
     */
    public static final String IGNITE_JOBS_HISTORY_SIZE = "IGNITE_JOBS_HISTORY_SIZE";

    /**
     * System property to override default job metrics processor property defining
     * concurrency level for structure holding job metrics snapshots.
     * Default value is {@code 64}.
     */
    public static final String IGNITE_JOBS_METRICS_CONCURRENCY_LEVEL = "IGNITE_JOBS_METRICS_CONCURRENCY_LEVEL";

    /**
     * System property to hold optional configuration URL.
     */
    public static final String IGNITE_CONFIG_URL = "IGNITE_CONFIG_URL";

    /** System property to hold SSH host for visor-started nodes. */
    public static final String IGNITE_SSH_HOST = "IGNITE_SSH_HOST";

    /** System property to enable experimental commands in control.sh script. */
    public static final String IGNITE_ENABLE_EXPERIMENTAL_COMMAND = "IGNITE_ENABLE_EXPERIMENTAL_COMMAND";

    /** System property to hold SSH user name for visor-started nodes. */
    public static final String IGNITE_SSH_USER_NAME = "IGNITE_SSH_USER_NAME";

    /** System property to hold preload resend timeout for evicted partitions. */
    public static final String IGNITE_PRELOAD_RESEND_TIMEOUT = "IGNITE_PRELOAD_RESEND_TIMEOUT";

    /**
     * System property to specify how often in milliseconds marshal buffers
     * should be rechecked and potentially trimmed. Default value is {@code 10,000ms}.
     */
    public static final String IGNITE_MARSHAL_BUFFERS_RECHECK = "IGNITE_MARSHAL_BUFFERS_RECHECK";

    /**
     * System property to disable {@link HostnameVerifier} for SSL connections.
     * Can be used for development with self-signed certificates. Default value is {@code false}.
     */
    public static final String IGNITE_DISABLE_HOSTNAME_VERIFIER = "IGNITE_DISABLE_HOSTNAME_VERIFIER";

    /**
     * System property to disable buffered communication if node sends less messages count than
     * specified by this property. Default value is {@code 512}.
     *
     * @deprecated Not used anymore.
     */
    @Deprecated
    public static final String IGNITE_MIN_BUFFERED_COMMUNICATION_MSG_CNT = "IGNITE_MIN_BUFFERED_COMMUNICATION_MSG_CNT";

    /**
     * Flag that will force Ignite to fill memory block with some recognisable pattern right before
     * this memory block is released. This will help to recognize cases when already released memory is accessed.
     */
    public static final String IGNITE_OFFHEAP_SAFE_RELEASE = "IGNITE_OFFHEAP_SAFE_RELEASE";

    /** Maximum size for atomic cache queue delete history (default is 200 000 entries per partition). */
    public static final String IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE = "IGNITE_ATOMIC_CACHE_DELETE_HISTORY_SIZE";

    /** Ttl of removed cache entries (ms). */
    public static final String IGNITE_CACHE_REMOVED_ENTRIES_TTL = "IGNITE_CACHE_REMOVED_ENTRIES_TTL";

    /**
     * Comma separated list of addresses in format "10.100.22.100:45000,10.100.22.101:45000".
     * Makes sense only for {@link org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder}.
     */
    public static final String IGNITE_TCP_DISCOVERY_ADDRESSES = "IGNITE_TCP_DISCOVERY_ADDRESSES";

    /**
     * Flag indicating whether performance suggestions output on start should be disabled.
     */
    public static final String IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED = "IGNITE_PERFORMANCE_SUGGESTIONS_DISABLED";

    /**
     * Flag indicating whether atomic operations allowed for use inside transactions.
     */
    public static final String IGNITE_ALLOW_ATOMIC_OPS_IN_TX = "IGNITE_ALLOW_ATOMIC_OPS_IN_TX";

    /**
     * Atomic cache deferred update response buffer size.
     */
    public static final String IGNITE_ATOMIC_DEFERRED_ACK_BUFFER_SIZE = "IGNITE_ATOMIC_DEFERRED_ACK_BUFFER_SIZE";

    /**
     * Atomic cache deferred update timeout.
     */
    public static final String IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT = "IGNITE_ATOMIC_DEFERRED_ACK_TIMEOUT";

    /**
     * Atomic cache deferred update timeout.
     */
    public static final String IGNITE_ATOMIC_CACHE_QUEUE_RETRY_TIMEOUT = "IGNITE_ATOMIC_CACHE_QUEUE_RETRY_TIMEOUT";

    /**
     * One phase commit deferred ack request timeout.
     */
    public static final String IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT =
        "IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_TIMEOUT";

    /**
     * One phase commit deferred ack request buffer size.
     */
    public static final String IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE =
        "IGNITE_DEFERRED_ONE_PHASE_COMMIT_ACK_REQUEST_BUFFER_SIZE";

    /**
     * If this property set then debug console will be opened for H2 indexing SPI.
     */
    public static final String IGNITE_H2_DEBUG_CONSOLE = "IGNITE_H2_DEBUG_CONSOLE";

    /**
     * This property allows to specify user defined port which H2 indexing SPI will use
     * to start H2 debug console on. If this property is not set or set to 0, H2 debug
     * console will use system-provided dynamic port.
     * This property is only relevant when {@link #IGNITE_H2_DEBUG_CONSOLE} property is set.
     */
    public static final String IGNITE_H2_DEBUG_CONSOLE_PORT = "IGNITE_H2_DEBUG_CONSOLE_PORT";

    /**
     * If this property is set to {@code true} then shared memory space native debug will be enabled.
     */
    public static final String IGNITE_IPC_SHMEM_SPACE_DEBUG = "IGNITE_IPC_SHMEM_SPACE_DEBUG";

    /**
     * Property allowing to skip configuration consistency checks.
     */
    public static final String IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK =
        "IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK";

    /**
     * Flag indicating whether validation of keys put to cache should be disabled.
     */
    public static final String IGNITE_CACHE_KEY_VALIDATION_DISABLED = "IGNITE_CACHE_KEY_VALIDATION_DISABLED";

    /**
     * Environment variable to override logging directory that has been set in logger configuration.
     */
    public static final String IGNITE_LOG_DIR = "IGNITE_LOG_DIR";

    /**
     * Environment variable to set work directory. The property {@link org.apache.ignite.configuration.IgniteConfiguration#setWorkDirectory} has higher
     * priority.
     */
    public static final String IGNITE_WORK_DIR = "IGNITE_WORK_DIR";

    /**
     * If this property is set to {@code true} then Ignite will append
     * hash code of {@link Ignite} class as hex string and append
     * JVM name returned by {@link RuntimeMXBean#getName()}.
     * <p>
     * This may be helpful when running Ignite in some application server
     * clusters or similar environments to avoid MBean name collisions.
     * <p>
     * Default is {@code false}.
     */
    public static final String IGNITE_MBEAN_APPEND_JVM_ID = "IGNITE_MBEAN_APPEND_JVM_ID";

    /**
     * If this property is set to {@code true} then Ignite will append
     * hash code of class loader to MXBean name.
     * <p>
     * Default is {@code true}.
     */
    public static final String IGNITE_MBEAN_APPEND_CLASS_LOADER_ID = "IGNITE_MBEAN_APPEND_CLASS_LOADER_ID";

    /**
     * If property is set to {@code true}, then Ignite will disable MBeans registration.
     * This may be helpful if MBeans are not allowed e.g. for security reasons.
     *
     * Default is {@code false}
     */
    public static final String IGNITE_MBEANS_DISABLED = "IGNITE_MBEANS_DISABLED";

    /**
     * If property is set to {@code true}, then test features will be enabled.
     *
     * Default is {@code false}.
     */
    public static final String IGNITE_TEST_FEATURES_ENABLED = "IGNITE_TEST_FEATURES_ENABLED";

    /**
     * Property controlling size of buffer holding last exception. Default value of {@code 1000}.
     */
    public static final String IGNITE_EXCEPTION_REGISTRY_MAX_SIZE = "IGNITE_EXCEPTION_REGISTRY_MAX_SIZE";

    /**
     * Property controlling default behavior of cache client flag.
     */
    public static final String IGNITE_CACHE_CLIENT = "IGNITE_CACHE_CLIENT";

    /**
     * Property controlling whether CacheManager will start grid with isolated IP finder when default URL
     * is passed in. This is needed to pass TCK tests which use default URL and assume isolated cache managers
     * for different class loaders.
     */
    public static final String IGNITE_JCACHE_DEFAULT_ISOLATED = "IGNITE_CACHE_CLIENT";

    /**
     * Property controlling maximum number of SQL result rows which can be fetched into a merge table.
     * If there are less rows than this threshold then multiple passes throw a table will be possible,
     * otherwise only one pass (e.g. only result streaming is possible).
     */
    public static final String IGNITE_SQL_MERGE_TABLE_MAX_SIZE = "IGNITE_SQL_MERGE_TABLE_MAX_SIZE";

    /**
     * Property controlling number of SQL result rows that will be fetched into a merge table at once before
     * applying binary search for the bounds.
     */
    public static final String IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE = "IGNITE_SQL_MERGE_TABLE_PREFETCH_SIZE";

    /** Disable fallback to H2 SQL parser if the internal SQL parser fails to parse the statement. */
    public static final String IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK = "IGNITE_SQL_PARSER_DISABLE_H2_FALLBACK";

    /** Force all SQL queries to be processed lazily regardless of what clients request. */
    public static final String IGNITE_SQL_FORCE_LAZY_RESULT_SET = "IGNITE_SQL_FORCE_LAZY_RESULT_SET";

    /** Disable SQL system views. */
    public static final String IGNITE_SQL_DISABLE_SYSTEM_VIEWS = "IGNITE_SQL_DISABLE_SYSTEM_VIEWS";

    /** Maximum size for affinity assignment history. */
    public static final String IGNITE_AFFINITY_HISTORY_SIZE = "IGNITE_AFFINITY_HISTORY_SIZE";

    /** Maximum size for discovery messages history. */
    public static final String IGNITE_DISCOVERY_HISTORY_SIZE = "IGNITE_DISCOVERY_HISTORY_SIZE";

    /** Maximum number of discovery message history used to support client reconnect. */
    public static final String IGNITE_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE =
        "IGNITE_DISCOVERY_CLIENT_RECONNECT_HISTORY_SIZE";

    /** Number of cache operation retries in case of topology exceptions. */
    public static final String IGNITE_CACHE_RETRIES_COUNT = "IGNITE_CACHE_RETRIES_COUNT";

    /** If this property is set to {@code true} then Ignite will log thread dump in case of partition exchange timeout. */
    public static final String IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT = "IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT";

    /** */
    public static final String IGNITE_IO_DUMP_ON_TIMEOUT = "IGNITE_IO_DUMP_ON_TIMEOUT";

    /** */
    public static final String IGNITE_DIAGNOSTIC_ENABLED = "IGNITE_DIAGNOSTIC_ENABLED";

    /** Cache operations that take more time than value of this property will be output to log. Set to {@code 0} to disable. */
    public static final String IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT = "IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT";

    /** Upper time limit between long running/hanging operations debug dumps. */
    public static final String IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT = "IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT_LIMIT";

    /** JDBC driver cursor remove delay. */
    public static final String IGNITE_JDBC_DRIVER_CURSOR_REMOVE_DELAY = "IGNITE_JDBC_DRIVER_CURSOR_RMV_DELAY";

    /** Long-long offheap map load factor. */
    public static final String IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR = "IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR";

    /** Maximum number of nested listener calls before listener notification becomes asynchronous. */
    public static final String IGNITE_MAX_NESTED_LISTENER_CALLS = "IGNITE_MAX_NESTED_LISTENER_CALLS";

    /** Indicating whether local store keeps primary only. Backward compatibility flag. */
    public static final String IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY = "IGNITE_LOCAL_STORE_KEEPS_PRIMARY_ONLY";

    /**
     * Manages {@link OptimizedMarshaller} behavior of {@code serialVersionUID} computation for
     * {@link Serializable} classes.
     */
    public static final String IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID =
        "IGNITE_OPTIMIZED_MARSHALLER_USE_DEFAULT_SUID";

    /**
     * Manages type of serialization mechanism for {@link String} that is marshalled/unmarshalled by BinaryMarshaller.
     * Should be used for cases when a String contains a surrogate symbol without its pair one. This is frequently used
     * in algorithms that encrypts data in String format.
     */
    public static final String IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2 =
        "IGNITE_BINARY_MARSHALLER_USE_STRING_SERIALIZATION_VER_2";

    /** Defines path to the file that contains list of classes allowed to safe deserialization.*/
    public static final String IGNITE_MARSHALLER_WHITELIST = "IGNITE_MARSHALLER_WHITELIST";

    /** Defines path to the file that contains list of classes disallowed to safe deserialization.*/
    public static final String IGNITE_MARSHALLER_BLACKLIST = "IGNITE_MARSHALLER_BLACKLIST";

    /**
     * If set to {@code true}, then default selected keys set is used inside
     * {@code GridNioServer} which lead to some extra garbage generation when
     * processing selected keys.
     * <p>
     * Default value is {@code false}. Should be switched to {@code true} if there are
     * any problems in communication layer.
     */
    public static final String IGNITE_NO_SELECTOR_OPTS = "IGNITE_NO_SELECTOR_OPTS";

    /**
     * System property to specify period in milliseconds between calls of the SQL statements cache cleanup task.
     * <p>
     * Cleanup tasks clears cache for terminated threads and for threads which did not perform SQL queries within
     * timeout configured via {@link #IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT} property.
     * <p>
     * Default value is {@code 10,000ms}.
     */
    public static final String IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD = "IGNITE_H2_INDEXING_CACHE_CLEANUP_PERIOD";

    /**
     * System property to specify timeout in milliseconds after which thread's SQL statements cache is cleared by
     * cleanup task if the thread does not perform any query.
     * <p>
     * Default value is {@code 600,000ms}.
     */
    public static final String IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT =
        "IGNITE_H2_INDEXING_CACHE_THREAD_USAGE_TIMEOUT";

    /**
     * Manages backward compatibility of {@link IgniteServices}. All nodes in cluster must have identical value
     * of this property.
     * <p>
     * If property is {@code false} then node is not required to have service implementation class if service is not
     * deployed on this node.
     * <p>
     * If the property is {@code true} then service implementation class is required on node even if service
     * is not deployed on this node.
     * <p>
     * If the property is not set ({@code null}) then Ignite will automatically detect which compatibility mode
     * should be used.
     */
    public static final String IGNITE_SERVICES_COMPATIBILITY_MODE = "IGNITE_SERVICES_COMPATIBILITY_MODE";

    /**
     * Manages backward compatibility of {@link StreamTransformer#from(CacheEntryProcessor)} method.
     * <p>
     * If the property is {@code true}, then the wrapped {@link CacheEntryProcessor} won't be able to be loaded over
     * P2P class loading.
     * <p>
     * If the property is {@code false}, then another implementation of {@link StreamTransformer} will be returned,
     * that fixes P2P class loading for {@link CacheEntryProcessor}, but it will be incompatible with old versions
     * of Ignite.
     */
    public static final String IGNITE_STREAM_TRANSFORMER_COMPATIBILITY_MODE =
        "IGNITE_STREAM_TRANSFORMER_COMPATIBILITY_MODE";

    /**
     * When set to {@code true} tree-based data structures - {@code TreeMap} and {@code TreeSet} - will not be
     * wrapped into special holders introduced to overcome serialization issue caused by missing {@code Comparable}
     * interface on {@code BinaryObject}.
     * <p>
     * @deprecated Should be removed in Apache Ignite 2.0.
     */
    @Deprecated
    public static final String IGNITE_BINARY_DONT_WRAP_TREE_STRUCTURES = "IGNITE_BINARY_DONT_WRAP_TREE_STRUCTURES";

    /**
     * When set to {@code true}, for consistent id will calculate by host name, without port, and you can use
     * only one node for host in cluster.
     */
    public static final String IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT = "IGNITE_CONSISTENT_ID_BY_HOST_WITHOUT_PORT";

    /** */
    public static final String IGNITE_IO_BALANCE_PERIOD = "IGNITE_IO_BALANCE_PERIOD";

    /**
     * When set to {@code true} fields are written by BinaryMarshaller in sorted order. Otherwise
     * the natural order is used.
     * <p>
     * @deprecated Should be removed in Apache Ignite 2.0.
     */
    @Deprecated
    public static final String IGNITE_BINARY_SORT_OBJECT_FIELDS = "IGNITE_BINARY_SORT_OBJECT_FIELDS";

    /**
     * Whether Ignite can access unaligned memory addresses.
     * <p>
     * Defaults to {@code false}, meaning that unaligned access will be performed only on x86 architecture.
     */
    public static final String IGNITE_MEMORY_UNALIGNED_ACCESS = "IGNITE_MEMORY_UNALIGNED_ACCESS";

    /**
     * When unsafe memory copy if performed below this threshold, Ignite will do it on per-byte basis instead of
     * calling to Unsafe.copyMemory().
     * <p>
     * Defaults to 0, meaning that threshold is disabled.
     */
    public static final String IGNITE_MEMORY_PER_BYTE_COPY_THRESHOLD = "IGNITE_MEMORY_PER_BYTE_COPY_THRESHOLD";

    /**
     * When set to {@code true} BinaryObject will be unwrapped before passing to IndexingSpi to preserve
     * old behavior query processor with IndexingSpi.
     * <p>
     * @deprecated Should be removed in Apache Ignite 2.0.
     */
    public static final String IGNITE_UNWRAP_BINARY_FOR_INDEXING_SPI = "IGNITE_UNWRAP_BINARY_FOR_INDEXING_SPI";

    /**
     * System property to specify maximum payload size in bytes for {@code H2TreeIndex}.
     * <p>
     * Defaults to {@code 0}, meaning that inline index store is disabled.
     */
    public static final String IGNITE_MAX_INDEX_PAYLOAD_SIZE = "IGNITE_MAX_INDEX_PAYLOAD_SIZE";

    /**
     * Time interval for calculating rebalance rate statistics, in milliseconds. Defaults to 60000.
     */
    public static final String IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL = "IGNITE_REBALANCE_STATISTICS_TIME_INTERVAL";

    /**
     * When cache has entries with expired TTL, each user operation will also remove this amount of expired entries.
     * Defaults to {@code 5}.
     */
    public static final String IGNITE_TTL_EXPIRE_BATCH_SIZE = "IGNITE_TTL_EXPIRE_BATCH_SIZE";

    /**
     * Indexing discovery history size. Protects from duplicate messages maintaining the list of IDs of recently
     * arrived discovery messages.
     * <p>
     * Defaults to {@code 1000}.
     */
    public static final String IGNITE_INDEXING_DISCOVERY_HISTORY_SIZE = "IGNITE_INDEXING_DISCOVERY_HISTORY_SIZE";

    /** Cache start size for on-heap maps. Defaults to 4096. */
    public static final String IGNITE_CACHE_START_SIZE = "IGNITE_CACHE_START_SIZE";

    /** */
    public static final String IGNITE_START_CACHES_ON_JOIN = "IGNITE_START_CACHES_ON_JOIN";

    /**
     * Skip CRC calculation flag.
     */
    public static final String IGNITE_PDS_SKIP_CRC = "IGNITE_PDS_SKIP_CRC";

    /**
     * WAL rebalance threshold.
     */
    public static final String IGNITE_PDS_PARTITION_DESTROY_CHECKPOINT_DELAY =
        "IGNITE_PDS_PARTITION_DESTROY_CHECKPOINT_DELAY";

    /**
     * WAL rebalance threshold.
     */
    public static final String IGNITE_PDS_WAL_REBALANCE_THRESHOLD = "IGNITE_PDS_WAL_REBALANCE_THRESHOLD";

    /** Ignite page memory concurrency level. */
    public static final String IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL = "IGNITE_OFFHEAP_LOCK_CONCURRENCY_LEVEL";

    /**
     * Start Ignite on versions of JRE 7 older than 1.7.0_71. For proper work it may require
     * disabling JIT in some places.
     */
    public static final String IGNITE_FORCE_START_JAVA7 = "IGNITE_FORCE_START_JAVA7";

    /**
     * When set to {@code true}, Ignite switches to compatibility mode with versions that don't
     * support service security permissions. In this case security permissions will be ignored
     * (if they set).
     * <p>
     *     Default is {@code false}, which means that service security permissions will be respected.
     * </p>
     */
    public static final String IGNITE_SECURITY_COMPATIBILITY_MODE = "IGNITE_SECURITY_COMPATIBILITY_MODE";

    /**
     * Ignite cluster name.
     * <p>
     * Defaults to utility cache deployment ID..
     */
    public static final String IGNITE_CLUSTER_NAME = "IGNITE_CLUSTER_NAME";

    /**
     * When client cache is started or closed special discovery message is sent to notify cluster (for example this is
     * needed for {@link ClusterGroup#forCacheNodes(String)} API. This timeout specifies how long to wait
     * after client cache start/close before sending this message. If during this timeout another client
     * cache changed, these events are combined into single message.
     * <p>
     * Default is 10 seconds.
     */
    public static final String IGNITE_CLIENT_CACHE_CHANGE_MESSAGE_TIMEOUT =
        "IGNITE_CLIENT_CACHE_CHANGE_MESSAGE_TIMEOUT";

    /**
     * If a partition release future completion time during an exchange exceeds this threshold, the contents of
     * the future will be dumped to the log on exchange. Default is {@code 0} (disabled).
     */
    public static final String IGNITE_PARTITION_RELEASE_FUTURE_DUMP_THRESHOLD =
        "IGNITE_PARTITION_RELEASE_FUTURE_DUMP_THRESHOLD";

    /**
     * If this property is set, a node will forcible fail a remote node when it fails to establish a communication
     * connection.
     */
    public static final String IGNITE_ENABLE_FORCIBLE_NODE_KILL = "IGNITE_ENABLE_FORCIBLE_NODE_KILL";

    /**
     * Tasks stealing will be started if tasks queue size per data-streamer thread exceeds this threshold.
     * <p>
     * Default value is {@code 4}.
     */
    public static final String IGNITE_DATA_STREAMING_EXECUTOR_SERVICE_TASKS_STEALING_THRESHOLD =
            "IGNITE_DATA_STREAMING_EXECUTOR_SERVICE_TASKS_STEALING_THRESHOLD";

    /**
     * If this property is set, then Ignite will use Async File IO factory by default.
     */
    public static final String IGNITE_USE_ASYNC_FILE_IO_FACTORY = "IGNITE_USE_ASYNC_FILE_IO_FACTORY";

    /**
     * If the property is set {@link org.apache.ignite.internal.pagemem.wal.record.TxRecord} records
     * will be logged to WAL.
     *
     * Default value is {@code false}.
     */
    public static final String IGNITE_WAL_LOG_TX_RECORDS = "IGNITE_WAL_LOG_TX_RECORDS";

    /** Max amount of remembered errors for {@link GridLogThrottle}. */
    public static final String IGNITE_LOG_THROTTLE_CAPACITY = "IGNITE_LOG_THROTTLE_CAPACITY";

    /** If this property is set, {@link DataStorageConfiguration#writeThrottlingEnabled} will be overridden to true
     * independent of initial value in configuration. */
    public static final String IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED = "IGNITE_OVERRIDE_WRITE_THROTTLING_ENABLED";

    /**
     * Property for setup WAL serializer version.
     */
    public static final String IGNITE_WAL_SERIALIZER_VERSION = "IGNITE_WAL_SERIALIZER_VERSION";

    /**
     * If the property is set Ignite will use legacy node comparator (based on node order) inste
     *
     * Default value is {@code false}.
     */
    public static final String IGNITE_USE_LEGACY_NODE_COMPARATOR = "IGNITE_USE_LEGACY_NODE_COMPARATOR";

    /**
     * Property that indicates should be mapped byte buffer used or not.
     * Possible values: {@code true} and {@code false}.
     */
    public static final String IGNITE_WAL_MMAP = "IGNITE_WAL_MMAP";

    /**
     * When set to {@code true}, Data store folders are generated only by consistent id, and no consistent ID will be
     * set based on existing data store folders. This option also enables compatible folder generation mode as it was
     * before 2.3.
     */
    public static final String IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID = "IGNITE_DATA_STORAGE_FOLDER_BY_CONSISTENT_ID";

    /** Ignite JVM pause detector disabled. */
    public static final String IGNITE_JVM_PAUSE_DETECTOR_DISABLED = "IGNITE_JVM_PAUSE_DETECTOR_DISABLED";

    /** Ignite JVM pause detector precision. */
    public static final String IGNITE_JVM_PAUSE_DETECTOR_PRECISION = "IGNITE_JVM_PAUSE_DETECTOR_PRECISION";

    /** Ignite JVM pause detector threshold. */
    public static final String IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD = "IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD";

    /** Ignite JVM pause detector last events count. */
    public static final String IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT = "IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT";

    /**
     * Default value is {@code false}.
     */
    public static final String IGNITE_WAL_DEBUG_LOG_ON_RECOVERY = "IGNITE_WAL_DEBUG_LOG_ON_RECOVERY";

    /**
     * Number of checkpoint history entries held in memory.
     */
    public static final String IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE = "IGNITE_PDS_MAX_CHECKPOINT_MEMORY_HISTORY_SIZE";

    /**
     * If this property is set to {@code true} enable logging in {@link GridClient}.
     */
    public static final String IGNITE_GRID_CLIENT_LOG_ENABLED = "IGNITE_GRID_CLIENT_LOG_ENABLED";

    /**
     * When set to {@code true}, direct IO may be enabled. Direct IO enabled only if JAR file with corresponding
     * feature is available in classpath and OS and filesystem settings allows to enable this mode.
     * Default is {@code true}.
     */
    public static final String IGNITE_DIRECT_IO_ENABLED = "IGNITE_DIRECT_IO_ENABLED";

    /**
     * When set to {@code true}, warnings that are intended for development environments and not for production
     * (such as coding mistakes in code using Ignite) will not be logged.
     */
    public static final String IGNITE_DEV_ONLY_LOGGING_DISABLED = "IGNITE_DEV_ONLY_LOGGING_DISABLED";

    /**
     * When set to {@code true} (default), pages are written to page store without holding segment lock (with delay).
     * Because other thread may require exactly the same page to be loaded from store, reads are protected by locking.
     */
    public static final String IGNITE_DELAYED_REPLACED_PAGE_WRITE = "IGNITE_DELAYED_REPLACED_PAGE_WRITE";

    /**
     * When set to {@code true}, WAL implementation with dedicated worker will be used even in FSYNC mode.
     * Default is {@code false}.
     */
    public static final String IGNITE_WAL_FSYNC_WITH_DEDICATED_WORKER = "IGNITE_WAL_FSYNC_WITH_DEDICATED_WORKER";

    /**
     * When set to {@code true}, on-heap cache cannot be enabled - see
     * {@link CacheConfiguration#setOnheapCacheEnabled(boolean)}.
     * Default is {@code false}.
     */
    public static final String IGNITE_DISABLE_ONHEAP_CACHE = "IGNITE_DISABLE_ONHEAP_CACHE";
    /**
     * When set to {@code false}, loaded pages implementation is switched to previous version of implementation,
     * FullPageIdTable. {@code True} value enables 'Robin Hood hashing: backward shift deletion'.
     * Default is {@code true}.
     */
    public static final String IGNITE_LOADED_PAGES_BACKWARD_SHIFT_MAP = "IGNITE_LOADED_PAGES_BACKWARD_SHIFT_MAP";

    /**
     * Whenever read load balancing is enabled, that means 'get' requests will be distributed between primary and backup
     * nodes if it is possible and {@link CacheConfiguration#readFromBackup} is {@code true}.
     *
     * Default is {@code true}.
     *
     * @see CacheConfiguration#readFromBackup
     */
    public static final String IGNITE_READ_LOAD_BALANCING = "IGNITE_READ_LOAD_BALANCING";

    /**
     * Number of repetitions to capture a lock in the B+Tree.
     */
    public static final String IGNITE_BPLUS_TREE_LOCK_RETRIES = "IGNITE_BPLUS_TREE_LOCK_RETRIES";

    /**
     * Amount of memory reserved in the heap at node start, which can be dropped to increase the chances of success when
     * handling OutOfMemoryError.
     *
     * Default is {@code 64kb}.
     */
    public static final String IGNITE_FAILURE_HANDLER_RESERVE_BUFFER_SIZE = "IGNITE_FAILURE_HANDLER_RESERVE_BUFFER_SIZE";

    /**
     * The threshold of uneven distribution above which partition distribution will be logged.
     *
     * The default is '50', that means: warn about nodes with 50+% difference.
     */
    public static final String IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD = "IGNITE_PART_DISTRIBUTION_WARN_THRESHOLD";

    /**
     * When set to {@code true}, WAL will be automatically disabled during rebalancing if there is no partition in
     * OWNING state.
     * Default is {@code false}.
     */
    public static final String IGNITE_DISABLE_WAL_DURING_REBALANCING = "IGNITE_DISABLE_WAL_DURING_REBALANCING";

    /**
     * Sets timeout for TCP client recovery descriptor reservation.
     */
    public static final String IGNITE_NIO_RECOVERY_DESCRIPTOR_RESERVATION_TIMEOUT =
            "IGNITE_NIO_RECOVERY_DESCRIPTOR_RESERVATION_TIMEOUT";

    /**
     * When set to {@code true}, Ignite will skip partitions sizes check on partition validation after rebalance has finished.
     * Partitions sizes may differs on nodes when Expiry Policy is in use and it is ok due to lazy entry eviction mechanics.
     *
     * There is no need to disable partition size validation either in normal case or when expiry policy is configured for cache.
     * But it should be disabled manually when policy is used on per entry basis to hint Ignite to skip this check.
     *
     * Default is {@code false}.
     */
    public static final String IGNITE_SKIP_PARTITION_SIZE_VALIDATION = "IGNITE_SKIP_PARTITION_SIZE_VALIDATION";

    /**
     * Enables threads dumping on critical node failure.
     *
     * Default is {@code true}.
     */
    public static final String IGNITE_DUMP_THREADS_ON_FAILURE = "IGNITE_DUMP_THREADS_ON_FAILURE";

    /**
     * Throttling timeout in millis which avoid excessive PendingTree access on unwind if there is nothing to clean yet.
     *
     * Default is 500 ms.
     */
    public static final String IGNITE_UNWIND_THROTTLING_TIMEOUT = "IGNITE_UNWIND_THROTTLING_TIMEOUT";

    /**
     * Enforces singleton.
     */
    private IgniteSystemProperties() {
        // No-op.
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @param name Name of the system property or environment variable.
     * @return Value of the system property or environment variable.
     *         Returns {@code null} if neither can be found for given name.
     */
    @Nullable public static String getString(String name) {
        assert name != null;

        String v = System.getProperty(name);

        if (v == null)
            v = System.getenv(name);

        return v;
    }

    /**
     * Gets either system property or environment variable with given name.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Value of the system property or environment variable.
     *         Returns {@code null} if neither can be found for given name.
     */
    @Nullable public static String getString(String name, String dflt) {
        String val = getString(name);

        return val == null ? dflt : val;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code boolean} using {@code Boolean.valueOf()} method.
     *
     * @param name Name of the system property or environment variable.
     * @return Boolean value of the system property or environment variable.
     *         Returns {@code False} in case neither system property
     *         nor environment variable with given name is found.
     */
    public static boolean getBoolean(String name) {
        return getBoolean(name, false);
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code boolean} using {@code Boolean.valueOf()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Boolean value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static boolean getBoolean(String name, boolean dflt) {
        String val = getString(name);

        return val == null ? dflt : Boolean.valueOf(val);
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code int} using {@code Integer.parseInt()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static int getInteger(String name, int dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        int res;

        try {
            res = Integer.parseInt(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code float} using {@code Float.parseFloat()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Float value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static float getFloat(String name, float dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        float res;

        try {
            res = Float.parseFloat(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code long} using {@code Long.parseLong()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static long getLong(String name, long dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        long res;

        try {
            res = Long.parseLong(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets either system property or environment variable with given name.
     * The result is transformed to {@code double} using {@code Double.parseDouble()} method.
     *
     * @param name Name of the system property or environment variable.
     * @param dflt Default value.
     * @return Integer value of the system property or environment variable.
     *         Returns default value in case neither system property
     *         nor environment variable with given name is found.
     */
    public static double getDouble(String name, double dflt) {
        String s = getString(name);

        if (s == null)
            return dflt;

        double res;

        try {
            res = Double.parseDouble(s);
        }
        catch (NumberFormatException ignore) {
            res = dflt;
        }

        return res;
    }

    /**
     * Gets snapshot of system properties.
     * Snapshot could be used for thread safe iteration over system properties.
     * Non-string properties are removed before return.
     *
     * @return Snapshot of system properties.
     */
    public static Properties snapshot() {
        Properties sysProps = (Properties)System.getProperties().clone();

        Iterator<Map.Entry<Object, Object>> iter = sysProps.entrySet().iterator();

        while (iter.hasNext()) {
            Map.Entry entry = iter.next();

            if (!(entry.getValue() instanceof String) || !(entry.getKey() instanceof String))
                iter.remove();
        }

        return sysProps;
    }
}
