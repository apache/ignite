/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;

/**
 * Contains constants for all system properties and environmental variables in GridGain. These
 * properties and variables can be used to affect the behavior of GridGain.
 */
public final class GridSystemProperties {
    /**
     * If this system property is present the GridGain will include grid name into verbose log.
     */
    public static final String GG_LOG_GRID_NAME = "GRIDGAIN_LOG_GRID_NAME";

    /**
     * This property is used internally to pass an exit code to loader when
     * GridGain instance is being restarted.
     */
    public static final String GG_RESTART_CODE = "GRIDGAIN_RESTART_CODE";

    /**
     * Presence of this system property with value {@code true} will make the grid
     * node start as a daemon node. Node that this system property will override
     * {@link GridConfiguration#isDaemon()} configuration.
     */
    public static final String GG_DAEMON = "GRIDGAIN_DAEMON";

    /** Defines GridGain installation folder. */
    public static final String GG_HOME = "GRIDGAIN_HOME";

    /** If this system property is set to {@code false} - no shutdown hook will be set. */
    public static final String GG_NO_SHUTDOWN_HOOK = "GRIDGAIN_NO_SHUTDOWN_HOOK";

    /**
     * Name of the system property to disable requirement for proper node ordering
     * by discovery SPI. Use with care, as proper node ordering is required for
     * cache consistency. If set to {@code true}, then any discovery SPI can be used
     * with distributed cache, otherwise, only discovery SPIs that have annotation
     * {@link GridDiscoverySpiOrderSupport @GridDiscoverySpiOrderSupport(true)} will
     * be allowed.
     */
    public static final String GG_NO_DISCO_ORDER = "GRIDGAIN_NO_DISCO_ORDER";

    /**
     * If this system property is set to {@code false} - no checks for new versions will
     * be performed by GridGain. By default, GridGain periodically checks for the new
     * version and prints out the message into the log if new version of GridGain is
     * available for download.
     */
    public static final String GG_UPDATE_NOTIFIER = "GRIDGAIN_UPDATE_NOTIFIER";

    /**
     * This system property defines interval in milliseconds in which GridGain will check
     * thread pool state for starvation. Zero value will disable this checker.
     */
    public static final String GG_STARVATION_CHECK_INTERVAL = "GRIDGAIN_STARVATION_CHECK_INTERVAL";

    /**
     * If this system property is present (any value) - no ASCII logo will
     * be printed.
     */
    public static final String GG_NO_ASCII = "GRIDGAIN_NO_ASCII";

    /**
     * This property allows to override Jetty host for REST processor.
     */
    public static final String GG_JETTY_HOST = "GRIDGAIN_JETTY_HOST";

    /**
     * This property allows to override Jetty local port for REST processor.
     */
    public static final String GG_JETTY_PORT = "GRIDGAIN_JETTY_PORT";

    /**
     * This property does not allow GridGain to override Jetty log configuration for REST processor.
     */
    public static final String GG_JETTY_LOG_NO_OVERRIDE = "GRIDGAIN_JETTY_LOG_NO_OVERRIDE";

    /**
     * This property allows to override maximum count of task results stored on one node
     * in REST processor.
     */
    public static final String GG_REST_MAX_TASK_RESULTS = "GRIDGAIN_REST_MAX_TASK_RESULTS";

    /**
     * This property defines the maximum number of attempts to remap near get to the same
     * primary node. Remapping may be needed when topology is changed concurrently with
     * get operation.
     */
    public static final String GG_NEAR_GET_MAX_REMAPS = "GRIDGAIN_NEAR_GET_MAX_REMAPS";

    /**
     * Set to either {@code true} or {@code false} to enable or disable quiet mode
     * of GridGain. In quiet mode, only warning and errors are printed into the log
     * additionally to a shortened version of standard output on the start.
     * <p>
     * Note that if you use <tt>ggstart.{sh|bat}</tt> scripts to start GridGain they
     * start by default in quiet mode. You can supply <tt>-v</tt> flag to override it.
     */
    public static final String GG_QUIET = "GRIDGAIN_QUIET";

    /**
     * If this property is set to {@code true} (default) and GridGain is launched
     * in verbose mode (see {@link #GG_QUIET}) and no console appenders can be found
     * in configuration, then default console appender will be added.
     * Set this property to {@code false} if no appenders should be added.
     */
    public static final String GG_CONSOLE_APPENDER = "GRIDGAIN_CONSOLE_APPENDER";

    /**
     * Name of the system property defining name of command line program.
     */
    public static final String GG_PROG_NAME = "GRIDGAIN_PROG_NAME";

    /**
     * Name of the system property defining success file name. This file
     * is used with auto-restarting functionality when GridGain is started
     * by supplied <tt>ggstart.{bat|sh}</tt> scripts.
     */
    public static final String GG_SUCCESS_FILE = "GRIDGAIN_SUCCESS_FILE";

    /**
     * Name of the system property or environment variable to set or override
     * SMTP host. If provided - it will override the property in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#getSmtpHost()
     */
    public static final String GG_SMTP_HOST = "GRIDGAIN_SMTP_HOST";

    /**
     * Name of the system property or environment variable to set or override
     * SMTP port. If provided - it will override the property in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#getSmtpPort()
     * @see GridConfiguration#DFLT_SMTP_PORT
     */
    public static final String GG_SMTP_PORT = "GRIDGAIN_SMTP_PORT";

    /**
     * Name of the system property or environment variable to set or override
     * SMTP username. If provided - it will override the property in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#getSmtpUsername()
     */
    public static final String GG_SMTP_USERNAME = "GRIDGAIN_SMTP_USERNAME";

    /**
     * Name of the system property or environment variable to set or override
     * SMTP password. If provided - it will override the property in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#getSmtpPassword()
     */
    public static final String GG_SMTP_PWD = "GRIDGAIN_SMTP_PASSWORD";

    /**
     * Name of the system property or environment variable to set or override
     * SMTP FROM email. If provided - it will override the property in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#getSmtpFromEmail()
     * @see GridConfiguration#DFLT_SMTP_FROM_EMAIL
     */
    public static final String GG_SMTP_FROM = "GRIDGAIN_SMTP_FROM";

    /**
     * Name of the system property or environment variable to set or override
     * list of admin emails. Value of this property should be comma-separated list
     * of emails. If provided - it will override the property in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#getAdminEmails()
     */
    public static final String GG_ADMIN_EMAILS = "GRIDGAIN_ADMIN_EMAILS";

    /**
     * Name of the system property or environment variable to set or override
     * whether or not to use SSL. If provided - it will override the property
     * in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#isSmtpSsl()
     * @see GridConfiguration#DFLT_SMTP_SSL
     */
    public static final String GG_SMTP_SSL = "GRIDGAIN_SMTP_SSL";

    /**
     * Name of the system property or environment variable to set or override
     * whether or not to enable email notifications for node lifecycle. If provided -
     * it will override the property in grid configuration.
     *
     * @see GridConfiguration#isLifeCycleEmailNotification()
     */
    public static final String GG_LIFECYCLE_EMAIL_NOTIFY = "GRIDGAIN_LIFECYCLE_EMAIL_NOTIFY";

    /**
     * Name of the system property or environment variable to set or override
     * whether or not to use STARTTLS. If provided - it will override the property
     * in grid configuration.
     * <p>
     * Note that GridGain uses SMTP to send emails in critical
     * situations such as license expiration or fatal system errors.
     * It is <b>highly</b> recommended to configure SMTP in production
     * environment.
     *
     * @see GridConfiguration#isSmtpStartTls()
     * @see GridConfiguration#DFLT_SMTP_STARTTLS
     */
    public static final String GG_SMTP_STARTTLS = "GRIDGAIN_SMTP_STARTTLS";

    /**
     * Name of system property to set system-wide local IP address or host. If provided it will
     * override all default local bind settings within GridGain or any of its SPIs.
     * <p>
     * Note that system-wide local bind address can also be set via {@link GridConfiguration#getLocalHost()}
     * method. However, system properties have priority over configuration properties specified in
     * {@link GridConfiguration}.
     */
    public static final String GG_LOCAL_HOST = "GRIDGAIN_LOCAL_HOST";

    /**
     * Name of the system property or environment variable to activate synchronous
     * listener notification for future objects implemented in GridGain. I.e.
     * closure passed into method {@link GridFuture#listenAsync(GridInClosure)} will
     * be evaluated in the same thread that will end the future.
     *
     * @see GridFuture#syncNotify()
     */
    public static final String GG_FUT_SYNC_NOTIFICATION = "GRIDGAIN_FUTURE_SYNC_NOTIFICATION";

    /**
     * Name of the system property or environment variable to activate concurrent
     * listener notification for future objects implemented in GridGain. I.e.
     * upon future completion every listener will be notified concurrently in a
     * separate thread.
     *
     * @see GridFuture#concurrentNotify()
     */
    public static final String GG_FUT_CONCURRENT_NOTIFICATION = "GRIDGAIN_FUTURE_CONCURRENT_NOTIFICATION";

    /**
     * System property to override deployment mode configuration parameter.
     * Valid values for property are: PRIVATE, ISOLATED, SHARED or CONTINUOUS.
     *
     * @see GridDeploymentMode
     * @see GridConfiguration#getDeploymentMode()
     */
    public static final String GG_DEP_MODE_OVERRIDE = "GRIDGAIN_DEPLOYMENT_MODE_OVERRIDE";

    /**
     * Property controlling size of buffer holding completed transaction versions. Such buffer
     * is used to detect duplicate transaction and has a default value of {@code 102400}. In
     * most cases this value is large enough and does not need to be changed.
     */
    public static final String GG_MAX_COMPLETED_TX_COUNT = "GRIDGAIN_MAX_COMPLETED_TX_COUNT";

    /**
     * Concurrency level for all concurrent hash maps created by GridGain.
     */
    public static final String GG_MAP_CONCURRENCY_LEVEL = "GRIDGAIN_MAP_CONCURRENCY_LEVEL";

    /**
     * Transactions that take more time, than value of this property, will be output to log
     * with warning level. {@code 0} (default value) disables warning on slow transactions.
     */
    public static final String GG_SLOW_TX_WARN_TIMEOUT = "GRIDGAIN_SLOW_TX_WARN_TIMEOUT";

    /**
     * Timeout after which all uncompleted transactions originated by left node will be
     * salvaged (i.e. invalidated and committed).
     */
    public static final String GG_TX_SALVAGE_TIMEOUT = "GRIDGAIN_TX_SALVAGE_TIMEOUT";

    /**
     * System property to override multicast group taken from configuration.
     * Used for testing purposes.
     */
    public static final String GG_OVERRIDE_MCAST_GRP = "GRIDGAIN_OVERRIDE_MCAST_GRP";

    /**
     * System property to override default reflection cache size. Default value is {@code 128}.
     */
    public static final String GG_REFLECTION_CACHE_SIZE = "GRIDGAIN_REFLECTION_CACHE_SIZE";

    /**
     * System property to override default job processor maps sizes for finished jobs and
     * cancellation requests. Default value is {@code 10240}.
     */
    public static final String GG_JOBS_HISTORY_SIZE = "GRIDGAIN_JOBS_HISTORY_SIZE";

    /**
     * System property to override default job metrics processor property defining
     * concurrency level for structure holding job metrics snapshots.
     * Default value is {@code 64}.
     */
    public static final String GG_JOBS_METRICS_CONCURRENCY_LEVEL = "GRIDGAIN_JOBS_METRICS_CONCURRENCY_LEVEL";

    /**
     * System property to hold optional configuration URL.
     */
    public static final String GG_CONFIG_URL = "GRIDGAIN_CONFIG_URL";

    /** System property to hold SSH host for visor-started nodes. */
    public static final String GG_SSH_HOST = "GRIDGAIN_SSH_HOST";

    /** System property to hold SSH user name for visor-started nodes. */
    public static final String GG_SSH_USER_NAME = "GRIDGAIN_SSH_USER_NAME";

    /** System property to hold preload resend timeout for evicted partitions. */
    public static final String GG_PRELOAD_RESEND_TIMEOUT = "GRIDGAIN_PRELOAD_RESEND_TIMEOUT";

    /**
     * System property to specify how often in milliseconds marshal buffers
     * should be rechecked and potentially trimmed. Default value is {@code 10,000ms}.
     */
    public static final String GG_MARSHAL_BUFFERS_RECHECK = "GRIDGAIN_MARSHAL_BUFFERS_RECHECK";

    /**
     * System property to disable {@link HostnameVerifier} for SSL connections.
     * Can be used for development with self-signed certificates. Default value is {@code false}.
     */
    public static final String GG_DISABLE_HOSTNAME_VERIFIER = "GRIDGAIN_DISABLE_HOSTNAME_VERIFIER";

    /**
     * System property to disable buffered communication if node sends less messages count than
     * specified by this property. Default value is {@code 512}.
     */
    public static final String GG_MIN_BUFFERED_COMMUNICATION_MSG_CNT = "GRIDGAIN_MIN_BUFFERED_COMMUNICATION_MSG_CNT";

    /**
     * System property to manage ratio for communication buffer resize. Buffer size will either
     * increase or decrease according to this ratio depending on system behavior. Default value is {@code 0.8}.
     */
    public static final String GG_COMMUNICATION_BUF_RESIZE_RATIO = "GRIDGAIN_COMMUNICATION_BUF_RESIZE_RATIO";

    /**
     * Flag that will force GridGain to fill memory block with some recognisable pattern right before
     * this memory block is released. This will help to recognize cases when already released memory is accessed.
     */
    public static final String GG_OFFHEAP_SAFE_RELEASE = "GRIDGAIN_OFFHEAP_SAFE_RELEASE";

    /** Maximum size for atomic cache queue delete history. */
    public static final String GG_ATOMIC_CACHE_DELETE_HISTORY_SIZE = "GRIDGAIN_ATOMIC_CACHE_DELETE_HISTORY_SIZE";

    /**
     * Comma separated list of addresses in format "10.100.22.100:45000,10.100.22.101:45000".
     * Makes sense only for {@link GridTcpDiscoveryVmIpFinder}.
     */
    public static final String GG_TCP_DISCOVERY_ADDRESSES = "GRIDGAIN_TCP_DISCOVERY_ADDRESSES";

    /**
     * Flag indicating whether performance suggestions output on start should be disabled.
     */
    public static final String GG_PERFORMANCE_SUGGESTIONS_DISABLED = "GRIDGAIN_PERFORMANCE_SUGGESTIONS_DISABLED";

    /**
     * Specifies log level for Visor client logging.
     */
    public static final String GG_VISOR_CLIENT_LOG_LEVEL = "GRIDGAIN_VISOR_CLIENT_LOG_LEVEL";

    /**
     * Atomic cache deferred update response buffer size.
     */
    public static final String GG_ATOMIC_DEFERRED_ACK_BUFFER_SIZE = "GRIDGAIN_ATOMIC_DEFERRED_ACK_BUFFER_SIZE";

    /**
     * Atomic cache deferred update timeout.
     */
    public static final String GG_ATOMIC_DEFERRED_ACK_TIMEOUT = "GRIDGAIN_ATOMIC_DEFERRED_ACK_TIMEOUT";

    /**
     * If this property set then debug console will be opened for H2 indexing SPI.
     */
    public static final String GG_H2_DEBUG_CONSOLE = "GRIDGAIN_H2_DEBUG_CONSOLE";

    /**
     * If this property is set to {@code true} then shared memory space native debug will be enabled.
     */
    public static final String GG_IPC_SHMEM_SPACE_DEBUG = "GRIDGAIN_IPC_SHMEM_SPACE_DEBUG";

    /**
     * Portable object array initial capacity.
     */
    public static final String GG_PORTABLE_ARRAY_INITIAL_CAPACITY = "GRIDGAIN_PORTABLE_ARRAY_INITIAL_CAPACITY";

    /**
     * Property allowing to skip configuration consistency checks.
     */
    public static final String GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK =
        "GRIDGAIN_SKIP_CONFIGURATION_CONSISTENCY_CHECK";

    /**
     * Flag indicating whether validation of keys put to cache should be disabled.
     */
    public static final String GG_CACHE_KEY_VALIDATION_DISABLED = "GRIDGAIN_CACHE_KEY_VALIDATION_DISABLED";

    /**
     * Enforces singleton.
     */
    private GridSystemProperties() {
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

    public static String getString(String name, String dflt) {
        String val = getString(name);

        return val == null ? dflt : val;
    }

    public static boolean getBoolean(String name) {
        return Boolean.valueOf(getString(name));
    }

    public static boolean getBoolean(String name, boolean dflt) {
        String val = getString(name);

        return val == null ? dflt : Boolean.valueOf(val);
    }

    public static int getInteger(String name) {
        String val = getString(name);
        return val == null ? 0 : Integer.valueOf(val);
    }

    public static int getInteger(String name, int dflt) {
        String val = getString(name);
        return val == null ? dflt : Integer.valueOf(val);
    }
}
