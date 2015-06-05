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

package org.apache.ignite.yarn;

import java.io.*;
import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

/**
 * Cluster settings.
 */
public class ClusterProperties {
    /** */
    private static final Logger log = Logger.getLogger(ClusterProperties.class.getSimpleName());

    /** */
    public static final String EMPTY_STRING = "";

    /** Unlimited. */
    public static final double UNLIMITED = Double.MAX_VALUE;

    /** */
    public static final String IGNITE_CLUSTER_NAME = "IGNITE_CLUSTER_NAME";

    /** */
    public static final String DEFAULT_CLUSTER_NAME = "ignite-cluster";

    /** Mesos master url. */
    private String clusterName = DEFAULT_CLUSTER_NAME;

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
    public static final String IGNITE_WORKING_DIR = "IGNITE_WORKING_DIR";

    /** */
    public static final String DEFAULT_IGNITE_WORK_DIR = "/ignite/workdir/";

    /** Ignite work directory. */
    private String igniteWorkDir = DEFAULT_IGNITE_WORK_DIR;

    /** */
    public static final String IGNITE_LOCAL_WORK_DIR = "IGNITE_LOCAL_WORK_DIR";

    /** */
    public static final String DEFAULT_IGNITE_LOCAL_WORK_DIR = "./ignite-releases/";

    /** Ignite local work directory. */
    private String igniteLocalWorkDir = DEFAULT_IGNITE_LOCAL_WORK_DIR;

    /** */
    public static final String IGNITE_RELEASES_DIR = "IGNITE_RELEASES_DIR";

    /** */
    public static final String DEFAULT_IGNITE_RELEASES_DIR = "/ignite/releases/";

    /** Ignite local work directory. */
    private String igniteReleasesDir = DEFAULT_IGNITE_RELEASES_DIR;

    /** */
    public static final String IGNITE_USERS_LIBS = "IGNITE_USERS_LIBS";

    /** Path to users libs. */
    private String userLibs = null;

    /** */
    public static final String IGNITE_USERS_LIBS_URL = "IGNITE_USERS_LIBS_URL";

    /** URL to users libs. */
    private String userLibsUrl = null;

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
     * @return Local working directory.
     */
    public String igniteLocalWorkDir() {
        return igniteLocalWorkDir;
    }

    /**
     * @return Ignite releases dir.
     */
    public String igniteReleasesDir() {
        return igniteReleasesDir;
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

            prop.clusterName = getStringProperty(IGNITE_CLUSTER_NAME, props, DEFAULT_CLUSTER_NAME);

            prop.userLibsUrl = getStringProperty(IGNITE_USERS_LIBS_URL, props, null);
            prop.igniteCfgUrl = getStringProperty(IGNITE_CONFIG_XML_URL, props, null);

            prop.cpu = getDoubleProperty(IGNITE_TOTAL_CPU, props, UNLIMITED);
            prop.cpuPerNode = getDoubleProperty(IGNITE_RUN_CPU_PER_NODE, props, UNLIMITED);
            prop.mem = getDoubleProperty(IGNITE_TOTAL_MEMORY, props, UNLIMITED);
            prop.memPerNode = getDoubleProperty(IGNITE_MEMORY_PER_NODE, props, UNLIMITED);
            prop.nodeCnt = getDoubleProperty(IGNITE_NODE_COUNT, props, UNLIMITED);
            prop.minCpu = getDoubleProperty(IGNITE_MIN_CPU_PER_NODE, props, DEFAULT_RESOURCE_MIN_CPU);
            prop.minMemory = getDoubleProperty(IGNITE_MIN_MEMORY_PER_NODE, props, DEFAULT_RESOURCE_MIN_MEM);

            prop.igniteVer = getStringProperty(IGNITE_VERSION, props, DEFAULT_IGNITE_VERSION);
            prop.igniteWorkDir = getStringProperty(IGNITE_WORKING_DIR, props, DEFAULT_IGNITE_WORK_DIR);
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
     * Convert to properties to map.
     *
     * @return Key-value map.
     */
    public Map<String, String> toEnvs() {
        Map<String, String> envs = new HashMap<>();

        envs.put(IGNITE_CLUSTER_NAME, toEnvVal(clusterName));

        envs.put(IGNITE_USERS_LIBS_URL, toEnvVal(userLibsUrl));
        envs.put(IGNITE_CONFIG_XML_URL, toEnvVal(igniteCfgUrl));

        envs.put(IGNITE_TOTAL_CPU, toEnvVal(cpu));
        envs.put(IGNITE_RUN_CPU_PER_NODE, toEnvVal(cpuPerNode));
        envs.put(IGNITE_TOTAL_MEMORY, toEnvVal(mem));
        envs.put(IGNITE_MEMORY_PER_NODE, toEnvVal(memPerNode));
        envs.put(IGNITE_NODE_COUNT, toEnvVal(nodeCnt));
        envs.put(IGNITE_MIN_CPU_PER_NODE, toEnvVal(minCpu));
        envs.put(IGNITE_MIN_MEMORY_PER_NODE, toEnvVal(minMemory));

        envs.put(IGNITE_VERSION, toEnvVal(igniteVer));
        envs.put(IGNITE_WORKING_DIR, toEnvVal(igniteWorkDir));
        envs.put(IGNITE_CONFIG_XML, toEnvVal(igniteCfg));
        envs.put(IGNITE_USERS_LIBS, toEnvVal(userLibs));

        if (hostnameConstraint != null)
            envs.put(IGNITE_HOSTNAME_CONSTRAINT, toEnvVal(hostnameConstraint.pattern()));

        return envs;
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

        return property == null || property.isEmpty() ? defaultVal : Double.valueOf(property);
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

        return property == null || property.isEmpty() ? defaultVal : property;
    }

    /**
     * @param val Value.
     * @return If val is null {@link EMPTY_STRING} else to string.
     */
    private String toEnvVal(Object val) {
        return val == null ? EMPTY_STRING : val.toString();
    }
}
