/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

/**
 * This class defines constants (NOT enums) for <b>internally-used</b> node attributes.
 */
public final class GridNodeAttributes {
    /** Prefix for internally reserved attribute names. */
    static final String ATTR_PREFIX = "org.gridgain";

    /** Internal attribute name constant. */
    public static final String ATTR_BUILD_VER = ATTR_PREFIX + ".build.ver";

    /** Node version attribute name. */
    public static final String ATTR_BUILD_DATE = ATTR_PREFIX + ".build.date";

    /** Internal attribute name constant. */
    public static final String ATTR_COMPATIBLE_VERS = ATTR_PREFIX + ".compatible.vers";

    /** Internal attribute name constant. */
    public static final String ATTR_MARSHALLER = ATTR_PREFIX + ".marshaller";

    /** Internal attribute name constant. */
    public static final String ATTR_JIT_NAME = ATTR_PREFIX + ".jit.name";

    /** Internal attribute name constant. */
    public static final String ATTR_LANG_RUNTIME = ATTR_PREFIX + ".lang.rt";

    /** Internal attribute name constant. */
    public static final String ATTR_USER_NAME = ATTR_PREFIX + ".user.name";

    /** Internal attribute name constant. */
    public static final String ATTR_GRID_NAME = ATTR_PREFIX + ".grid.name";

    /** Deployment mode. */
    public static final String ATTR_DEPLOYMENT_MODE = ATTR_PREFIX + ".grid.dep.mode";

    /** Peer classloading enabled flag. */
    public static final String ATTR_PEER_CLASSLOADING = ATTR_PREFIX + ".peer.classloading.enabled";

    /** Internal attribute name postfix constant. */
    public static final String ATTR_SPI_CLASS = ATTR_PREFIX + ".spi.class";

    /** Internal attribute name constant. */
    public static final String ATTR_CACHE = ATTR_PREFIX + ".cache";

    /** Internal attribute name constant. */
    @Deprecated
    public static final String ATTR_CACHE_PORTABLE = ATTR_PREFIX + ".cache.portable";

    /** Internal attribute name constant. */
    public static final String ATTR_GGFS = ATTR_PREFIX + ".ggfs";

    /** Internal attribute name constant. */
    public static final String ATTR_MONGO = ATTR_PREFIX + ".mongo";

    /** Internal attribute name constant. */
    public static final String ATTR_DAEMON = ATTR_PREFIX + ".daemon";

    /** Internal attribute name constant. */
    public static final String ATTR_JMX_PORT = ATTR_PREFIX + ".jmx.port";

    /** Internal attribute name constant. */
    public static final String ATTR_RESTART_ENABLED = ATTR_PREFIX + ".restart.enabled";

    /** Internal attribute name constant. */
    public static final String ATTR_REST_TCP_ADDRS = ATTR_PREFIX + ".rest.tcp.addrs";

    /** Internal attribute name constant. */
    public static final String ATTR_REST_TCP_HOST_NAMES = ATTR_PREFIX + ".rest.tcp.host.names";

    /** Internal attribute name constant. */
    public static final String ATTR_REST_TCP_PORT = ATTR_PREFIX + ".rest.tcp.port";

    /** Internal attribute name constant */
    public static final String ATTR_REST_PORT_RANGE = ATTR_PREFIX + ".rest.port.range";

    /** Internal attribute name constant */
    public static final String ATTR_REST_JETTY_ADDRS = ATTR_PREFIX + ".rest.jetty.addrs";

    /** Internal attribute name constant */
    public static final String ATTR_REST_JETTY_HOST_NAMES = ATTR_PREFIX + ".rest.jetty.host.names";

    /** Internal attribute name constant */
    public static final String ATTR_REST_JETTY_PORT = ATTR_PREFIX + ".rest.jetty.port";

    /** Internal attribute name constant. */
    public static final String ATTR_IPS = ATTR_PREFIX + ".ips";

    /** Internal attribute name constant. */
    public static final String ATTR_MACS = ATTR_PREFIX + ".macs";

    /** Internal attribute name constant. */
    public static final String ATTR_PHY_RAM = ATTR_PREFIX + ".phy.ram";

    /** Internal attribute name constant. */
    public static final String ATTR_JVM_PID = ATTR_PREFIX + ".jvm.pid";

    /** Internal attribute name constant. */
    public static final String ATTR_JVM_ARGS = ATTR_PREFIX + ".jvm.args";

    /** Internal attribute name constant. */
    public static final String ATTR_STREAMER = ATTR_PREFIX + ".streamer";

    /** Time server host attribute name. */
    public static final String ATTR_TIME_SERVER_HOST = ATTR_PREFIX + ".time.host";

    /** Time server port attribute name. */
    public static final String ATTR_TIME_SERVER_PORT = ATTR_PREFIX + ".time.port";

    /** Internal attribute name constant. */
    public static final String ATTR_REPLICATION_SND_HUB = ATTR_PREFIX + ".replication.snd.hub";

    /** Internal attribute name constant. */
    public static final String ATTR_REPLICATION_CACHES = ATTR_PREFIX + ".replication.caches";

    /** Version converters attribute name. */
    public static final String ATTR_VER_CONVERTERS = ATTR_PREFIX + ".ver.converters";

    /** Internal attribute name constant. */
    public static final String ATTR_DATA_CENTER_ID = ATTR_PREFIX + ".data.center.id";

    /** Security credentials attribute name. Attribute is not available via public API. */
    public static final String ATTR_SECURITY_CREDENTIALS = ATTR_PREFIX + ".security.cred";

    /** Security subject for authenticated node. */
    public static final String ATTR_SECURITY_SUBJECT = ATTR_PREFIX + ".security.subject";

    /** Cache interceptors. */
    public static final String ATTR_CACHE_INTERCEPTORS = ATTR_PREFIX + ".cache.interceptors";

    /**
     * Enforces singleton.
     */
    private GridNodeAttributes() {
        /* No-op. */
    }
}
