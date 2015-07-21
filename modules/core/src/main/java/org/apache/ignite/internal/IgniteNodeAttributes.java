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

package org.apache.ignite.internal;

/**
 * This class defines constants (NOT enums) for <b>internally-used</b> node attributes.
 */
public final class IgniteNodeAttributes {
    /** Prefix for internally reserved attribute names. */
    public static final String ATTR_PREFIX = "org.apache.ignite";

    /** Node compound version. */
    public static final String ATTR_BUILD_VER = ATTR_PREFIX + ".build.ver";

    /** Internal attribute name constant. */
    public static final String ATTR_BUILD_DATE = ATTR_PREFIX + ".build.date";

    /** Internal attribute name constant. */
    public static final String ATTR_MARSHALLER = ATTR_PREFIX + ".marshaller";

    /** Internal attribute name constant. */
    public static final String ATTR_JIT_NAME = ATTR_PREFIX + ".jit.name";

    /** Internal attribute name constant. */
    public static final String ATTR_LANG_RUNTIME = ATTR_PREFIX + ".lang.rt";

    /** Internal attribute name constant. */
    public static final String ATTR_USER_NAME = ATTR_PREFIX + ".user.name";

    /** Internal attribute name constant. */
    public static final String ATTR_GRID_NAME = ATTR_PREFIX + ".ignite.name";

    /** Deployment mode. */
    public static final String ATTR_DEPLOYMENT_MODE = ATTR_PREFIX + ".ignite.dep.mode";

    /** Peer classloading enabled flag. */
    public static final String ATTR_PEER_CLASSLOADING = ATTR_PREFIX + ".peer.classloading.enabled";

    /** Internal attribute name postfix constant. */
    public static final String ATTR_SPI_CLASS = ATTR_PREFIX + ".spi.class";

    /** Internal attribute name constant. */
    public static final String ATTR_CACHE = ATTR_PREFIX + ".cache";

    /** Internal attribute name constant. */
    public static final String ATTR_TX_CONFIG = ATTR_PREFIX + ".tx";

    /** Internal attribute name constant. */
    public static final String ATTR_IGFS = ATTR_PREFIX + ".igfs";

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

    /** Security credentials attribute name. Attribute is not available via public API. */
    public static final String ATTR_SECURITY_CREDENTIALS = ATTR_PREFIX + ".security.cred";

    /** Security subject for authenticated node. */
    public static final String ATTR_SECURITY_SUBJECT = ATTR_PREFIX + ".security.subject";

    /** Client mode flag. */
    public static final String ATTR_CLIENT_MODE = ATTR_PREFIX + ".cache.client";

    /** Configuration consistency check disabled flag. */
    public static final String ATTR_CONSISTENCY_CHECK_SKIPPED = ATTR_PREFIX + ".consistency.check.skipped";

    /** Node consistent id. */
    public static final String ATTR_NODE_CONSISTENT_ID = ATTR_PREFIX + ".consistent.id";

    /**
     * Enforces singleton.
     */
    private IgniteNodeAttributes() {
        /* No-op. */
    }
}
