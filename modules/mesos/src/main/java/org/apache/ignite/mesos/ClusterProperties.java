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

package org.apache.ignite.mesos;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

/**
 * Cluster settings.
 */
public class ClusterProperties {
    /** */
    private static final Logger log = Logger.getLogger(ClusterProperties.class.getSimpleName());

    /** Unlimited. */
    public static final double UNLIMITED = Double.MAX_VALUE;

    /** */
    public static final String MESOS_MASTER_URL = "MESOS_MASTER_URL";

    /** */
    public static final String DEFAULT_MESOS_MASTER_URL = "zk://localhost:2181/mesos";

    /** Mesos master url. */
    private String mesosUrl = DEFAULT_MESOS_MASTER_URL;

    /** */
    public static final String IGNITE_JVM_OPTS = "IGNITE_JVM_OPTS";

    /** JVM options. */
    private String jvmOpts = "";

    /** */
    public static final String IGNITE_CLUSTER_NAME = "IGNITE_CLUSTER_NAME";

    /** */
    public static final String DEFAULT_CLUSTER_NAME = "ignite-cluster";

    /** Mesos master url. */
    private String clusterName = DEFAULT_CLUSTER_NAME;

    /** */
    public static final String IGNITE_HTTP_SERVER_HOST = "IGNITE_HTTP_SERVER_HOST";

    /** Http server host. */
    private String httpServerHost = null;

    /** */
    public static final String IGNITE_HTTP_SERVER_PORT = "IGNITE_HTTP_SERVER_PORT";

    /** */
    public static final String DEFAULT_HTTP_SERVER_PORT = "48610";

    /** Http server host. */
    private int httpServerPort = Integer.valueOf(DEFAULT_HTTP_SERVER_PORT);

    /** */
    public static final String IGNITE_TOTAL_CPU = "IGNITE_TOTAL_CPU";

    /** CPU limit. */
    private double cpu = UNLIMITED;

    /** */
    public static final String IGNITE_RUN_CPU_PER_NODE = "IGNITE_RUN_CPU_PER_NODE";

    /** CPU limit. */
    private double cpuPerNode = UNLIMITED;

    /** */
    public static final String IGNITE_TOTAL_MEMORY = "IGNITE_TOTAL_MEMORY";

    /** Memory limit. */
    private double mem = UNLIMITED;

    /** */
    public static final String IGNITE_MEMORY_PER_NODE = "IGNITE_MEMORY_PER_NODE";

    /** Memory limit. */
    private double memPerNode = UNLIMITED;

    /** */
    public static final String IGNITE_TOTAL_DISK_SPACE = "IGNITE_TOTAL_DISK_SPACE";

    /** Disk space limit. */
    private double disk = UNLIMITED;

    /** */
    public static final String IGNITE_DISK_SPACE_PER_NODE = "IGNITE_DISK_SPACE_PER_NODE";

    /** Disk space limit. */
    private double diskPerNode = UNLIMITED;

    /** */
    public static final String IGNITE_NODE_COUNT = "IGNITE_NODE_COUNT";

    /** Node count limit. */
    private double nodeCnt = UNLIMITED;

    /** */
    public static final String IGNITE_MIN_CPU_PER_NODE = "IGNITE_MIN_CPU_PER_NODE";

    /** */
    public static final double DEFAULT_RESOURCE_MIN_CPU = 1;

    /** Min memory per node. */
    private double minCpu = DEFAULT_RESOURCE_MIN_CPU;

    /** */
    public static final String IGNITE_MIN_MEMORY_PER_NODE = "IGNITE_MIN_MEMORY_PER_NODE";

    /** */
    public static final double DEFAULT_RESOURCE_MIN_MEM = 256;

    /** Min memory per node. */
    private double minMemory = DEFAULT_RESOURCE_MIN_MEM;

    /** */
    public static final String IGNITE_VERSION = "IGNITE_VERSION";

    /** */
    public static final String DEFAULT_IGNITE_VERSION = "latest";

    /** Ignite version. */
    private String igniteVer = DEFAULT_IGNITE_VERSION;

    /** */
    public static final String IGNITE_PACKAGE_URL = "IGNITE_PACKAGE_URL";

    /** Ignite package url. */
    private String ignitePackageUrl = null;

    /** */
    public static final String IGNITE_WORK_DIR = "IGNITE_WORK_DIR";

    /** */
    public static final String DEFAULT_IGNITE_WORK_DIR = "ignite-releases/";

    /** Ignite version. */
    private String igniteWorkDir = DEFAULT_IGNITE_WORK_DIR;

    /** */
    public static final String IGNITE_USERS_LIBS = "IGNITE_USERS_LIBS";

    /** Path to users libs. */
    private String userLibs = null;

    /** */
    public static final String IGNITE_USERS_LIBS_URL = "IGNITE_USERS_LIBS_URL";

    /** URL to users libs. */
    private String userLibsUrl = null;

    /** */
    public static final String LICENCE_URL = "LICENCE_URL";

    /** Licence url. */
    private String licenceUrl = null;

    /** */
    public static final String IGNITE_CONFIG_XML = "IGNITE_XML_CONFIG";

    /** Ignite config. */
    private String igniteCfg = null;

    /** */
    public static final String IGNITE_CONFIG_XML_URL = "IGNITE_CONFIG_XML_URL";

    /** Url to ignite config. */
    private String igniteCfgUrl = null;

    /** */
    public static final String IGNITE_HOSTNAME_CONSTRAINT = "IGNITE_HOSTNAME_CONSTRAINT";

    /** Url to ignite config. */
    private Pattern hostnameConstraint = null;

    /** */
    public ClusterProperties() {
        // No-op.
    }

    /**
     * @return Cluster name.
     */
    public String clusterName() {
        return clusterName;
    }

    /**
     * @return CPU count limit.
     */
    public double cpus() {
        return cpu;
    }

    /**
     * Sets CPU count limit.
     */
    public void cpus(double cpu) {
        this.cpu = cpu;
    }

    /**
     * @return CPU count limit.
     */
    public double cpusPerNode() {
        return cpuPerNode;
    }

    /**
     * Sets CPU count limit.
     */
    public void cpusPerNode(double cpu) {
        this.cpuPerNode = cpu;
    }

    /**
     * @return mem limit.
     */
    public double memory() {
        return mem;
    }

    /**
     * Sets mem limit.
     *
     * @param mem Memory.
     */
    public void memory(double mem) {
        this.mem = mem;
    }

    /**
     * @return mem limit.
     */
    public double memoryPerNode() {
        return memPerNode;
    }

    /**
     * Sets mem limit.
     *
     * @param mem Memory.
     */
    public void memoryPerNode(double mem) {
         this.memPerNode = mem;
    }

    /**
     * @return JVM opts for ignite.
     */
    public String jmvOpts() {
        return this.jvmOpts;
    }

    /**
     * @return disk limit.
     */
    public double disk() {
        return disk;
    }

    /**
     * @return disk limit per node.
     */
    public double diskPerNode() {
        return diskPerNode;
    }

    /**
     * @return instance count limit.
     */
    public double instances() {
        return nodeCnt;
    }

    /**
     * @return min memory per node.
     */
    public double minMemoryPerNode() {
        return minMemory;
    }

    /**
     * Sets min memory.
     *
     * @param minMemory Min memory.
     */
    public void minMemoryPerNode(double minMemory) {
        this.minMemory = minMemory;
    }

    /**
     * Sets hostname constraint.
     *
     * @param pattern Hostname pattern.
     */
    public void hostnameConstraint(Pattern pattern) {
        this.hostnameConstraint = pattern;
    }

    /**
     * @return min cpu count per node.
     */
    public double minCpuPerNode() {
        return minCpu;
    }

    /**
     * Sets min cpu count per node.
     *
     * @param minCpu min cpu count per node.
     */
    public void minCpuPerNode(double minCpu) {
        this.minCpu = minCpu;
    }

    /**
     * @return Ignite version.
     */
    public String igniteVer() {
        return igniteVer;
    }

    /**
     * @return Working directory.
     */
    public String igniteWorkDir() {
        return igniteWorkDir;
    }

    /**
     * @return User's libs.
     */
    public String userLibs() {
        return userLibs;
    }

    /**
     * @return Ignite configuration.
     */
    public String igniteCfg() {
        return igniteCfg;
    }

    /**
     * @return Master url.
     */
    public String masterUrl() {
        return mesosUrl;
    }

    /**
     * @return Http server host.
     */
    public String httpServerHost() {
        return httpServerHost;
    }

    /**
     * @return Http server port.
     */
    public int httpServerPort() {
        return httpServerPort;
    }

    /**
     * @return Url to ignite package.
     */
    public String ignitePackageUrl() {
        return ignitePackageUrl;
    }

    /**
     * @return Url to ignite configuration.
     */
    public String igniteConfigUrl() {
        return igniteCfgUrl;
    }

    /**
     * @return Url to users libs configuration.
     */
    public String usersLibsUrl() {
        return userLibsUrl;
    }

    /**
     * @return Url to licence.
     */
    public String licenceUrl() {
        return licenceUrl;
    }

    /**
     * @return Host name constraint.
     */
    public Pattern hostnameConstraint() {
        return hostnameConstraint;
    }

    /**
     * @param config path to config file.
     * @return Cluster configuration.
     */
    public static ClusterProperties from(String config) {
        try {
            Properties props = null;

            if (config != null) {
                props = new Properties();

                props.load(new FileInputStream(config));
            }

            ClusterProperties prop = new ClusterProperties();

            prop.mesosUrl = getStringProperty(MESOS_MASTER_URL, props, DEFAULT_MESOS_MASTER_URL);

            prop.httpServerHost = getStringProperty(IGNITE_HTTP_SERVER_HOST, props, getNonLoopbackAddress());

            String port = System.getProperty("PORT0");

            if (port != null && !port.isEmpty())
                prop.httpServerPort = Integer.valueOf(port);
            else
                prop.httpServerPort = Integer.valueOf(getStringProperty(IGNITE_HTTP_SERVER_PORT, props,
                    DEFAULT_HTTP_SERVER_PORT));

            prop.clusterName = getStringProperty(IGNITE_CLUSTER_NAME, props, DEFAULT_CLUSTER_NAME);

            prop.userLibsUrl = getStringProperty(IGNITE_USERS_LIBS_URL, props, null);
            prop.ignitePackageUrl = getStringProperty(IGNITE_PACKAGE_URL, props, null);
            prop.licenceUrl = getStringProperty(LICENCE_URL, props, null);
            prop.igniteCfgUrl = getStringProperty(IGNITE_CONFIG_XML_URL, props, null);

            prop.cpu = getDoubleProperty(IGNITE_TOTAL_CPU, props, UNLIMITED);
            prop.cpuPerNode = getDoubleProperty(IGNITE_RUN_CPU_PER_NODE, props, UNLIMITED);
            prop.mem = getDoubleProperty(IGNITE_TOTAL_MEMORY, props, UNLIMITED);
            prop.memPerNode = getDoubleProperty(IGNITE_MEMORY_PER_NODE, props, UNLIMITED);
            prop.disk = getDoubleProperty(IGNITE_TOTAL_DISK_SPACE, props, UNLIMITED);
            prop.diskPerNode = getDoubleProperty(IGNITE_DISK_SPACE_PER_NODE, props, 1024.0);
            prop.nodeCnt = getDoubleProperty(IGNITE_NODE_COUNT, props, UNLIMITED);
            prop.minCpu = getDoubleProperty(IGNITE_MIN_CPU_PER_NODE, props, DEFAULT_RESOURCE_MIN_CPU);
            prop.minMemory = getDoubleProperty(IGNITE_MIN_MEMORY_PER_NODE, props, DEFAULT_RESOURCE_MIN_MEM);

            prop.jvmOpts = getStringProperty(IGNITE_JVM_OPTS, props, "");

            prop.igniteVer = getStringProperty(IGNITE_VERSION, props, DEFAULT_IGNITE_VERSION);
            prop.igniteWorkDir = getStringProperty(IGNITE_WORK_DIR, props, DEFAULT_IGNITE_WORK_DIR);
            prop.igniteCfg = getStringProperty(IGNITE_CONFIG_XML, props, null);
            prop.userLibs = getStringProperty(IGNITE_USERS_LIBS, props, null);

            String pattern = getStringProperty(IGNITE_HOSTNAME_CONSTRAINT, props, null);

            if (pattern != null) {
                try {
                    prop.hostnameConstraint = Pattern.compile(pattern);
                }
                catch (PatternSyntaxException e) {
                    log.log(Level.WARNING, "IGNITE_HOSTNAME_CONSTRAINT has invalid pattern. It will be ignore.", e);
                }
            }

            return prop;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param name Property name.
     * @param fileProps Property file.
     * @return Property value.
     */
    private static double getDoubleProperty(String name, Properties fileProps, Double defaultVal) {
        if (fileProps != null && fileProps.containsKey(name))
            return Double.valueOf(fileProps.getProperty(name));

        String property = System.getProperty(name);

        if (property == null)
            property = System.getenv(name);

        return property == null ? defaultVal : Double.valueOf(property);
    }

    /**
     * @param name Property name.
     * @param fileProps Property file.
     * @return Property value.
     */
    private static String getStringProperty(String name, Properties fileProps, String defaultVal) {
        if (fileProps != null && fileProps.containsKey(name))
            return fileProps.getProperty(name);

        String property = System.getProperty(name);

        if (property == null)
            property = System.getenv(name);

        return property == null ? defaultVal : property;
    }

    /**
     * Finds a local, non-loopback, IPv4 address
     *
     * @return The first non-loopback IPv4 address found, or <code>null</code> if no such addresses found
     * @throws java.net.SocketException If there was a problem querying the network interfaces
     */
    public static String getNonLoopbackAddress() throws SocketException {
        Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();

        while (ifaces.hasMoreElements()) {
            NetworkInterface iface = ifaces.nextElement();

            Enumeration<InetAddress> addresses = iface.getInetAddresses();

            while (addresses.hasMoreElements()) {
                InetAddress addr = addresses.nextElement();

                if (addr instanceof Inet4Address && !addr.isLoopbackAddress())
                    return addr.getHostAddress();
            }
        }

        throw new RuntimeException("Failed. Couldn't find non-loopback address");
    }
}
