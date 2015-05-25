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
import java.util.*;

/**
 * Cluster settings.
 */
public class ClusterProperties {
    /** Unlimited. */
    public static final double UNLIMITED = -1;

    /** */
    public static final String MESOS_MASTER_URL = "MESOS_MASTER_URL";

    /** */
    public static final String DEFAULT_MESOS_MASTER_URL = "zk://localhost:2181/mesos";

    /** Mesos master url. */
    private String mesosUrl = DEFAULT_MESOS_MASTER_URL;

    /** */
    public static final String IGNITE_RESOURCE_CPU_CORES = "IGNITE_RESOURCE_CPU_CORES";

    /** CPU limit. */
    private double cpu = UNLIMITED;

    /** */
    public static final String IGNITE_RESOURCE_MEM_MB = "IGNITE_RESOURCE_MEM_MB";

    /** Memory limit. */
    private double mem = UNLIMITED;

    /** */
    public static final String IGNITE_RESOURCE_DISK_MB = "IGNITE_RESOURCE_DISK_MB";

    /** Disk space limit. */
    private double disk = UNLIMITED;

    /** */
    public static final String IGNITE_RESOURCE_NODE_CNT = "IGNITE_RESOURCE_NODE_CNT";

    /** Node count limit. */
    private double nodeCnt = UNLIMITED;

    /** */
    public static final String IGNITE_RESOURCE_MIN_CPU_CNT_PER_NODE = "IGNITE_RESOURCE_MIN_CPU_CNT_PER_NODE";

    /** */
    public static final double DEFAULT_RESOURCE_MIN_CPU = 2;

    /** Min memory per node. */
    private double minCpu = DEFAULT_RESOURCE_MIN_CPU;

    /** */
    public static final String IGNITE_RESOURCE_MIN_MEMORY_PER_NODE = "IGNITE_RESOURCE_MIN_MEMORY_PER_NODE";

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
    public static final String IGNITE_WORK_DIR = "IGNITE_WORK_DIR";

    /** */
    public static final String DEFAULT_IGNITE_WORK_DIR = "~/ignite-releases";

    /** Ignite version. */
    private String igniteWorkDir = DEFAULT_IGNITE_WORK_DIR;

    /** */
    public static final String IGNITE_USERS_LIBS = "IGNITE_USERS_LIBS";

    /** Path to users libs. */
    private String userLibs = null;

    /** */
    public static final String IGNITE_CONFIG_XML = "IGNITE_XML_CONFIG";

    /** Ignite config. */
    private String igniteCfg = null;

    /** */
    public ClusterProperties() {
        // No-op.
    }

    /**
     * @return CPU count limit.
     */
    public double cpus(){
        return cpu;
    }

    /**
     * @return mem limit.
     */
    public double memory() {
        return mem;
    }

    /**
     * @return disk limit.
     */
    public double disk() {
        return disk;
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
     * @return min cpu count per node.
     */
    public double minCpuPerNode() {
        return minCpu;
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

            prop.cpu = getDoubleProperty(IGNITE_RESOURCE_CPU_CORES, props, UNLIMITED);
            prop.mem = getDoubleProperty(IGNITE_RESOURCE_MEM_MB, props, UNLIMITED);
            prop.disk = getDoubleProperty(IGNITE_RESOURCE_DISK_MB, props, UNLIMITED);
            prop.nodeCnt = getDoubleProperty(IGNITE_RESOURCE_NODE_CNT, props, UNLIMITED);
            prop.minCpu = getDoubleProperty(IGNITE_RESOURCE_MIN_CPU_CNT_PER_NODE, props, DEFAULT_RESOURCE_MIN_CPU);
            prop.minMemory = getDoubleProperty(IGNITE_RESOURCE_MIN_MEMORY_PER_NODE, props, DEFAULT_RESOURCE_MIN_MEM);

            prop.igniteVer = getStringProperty(IGNITE_VERSION, props, DEFAULT_IGNITE_VERSION);
            prop.igniteWorkDir = getStringProperty(IGNITE_WORK_DIR, props, DEFAULT_IGNITE_WORK_DIR);
            prop.igniteCfg = getStringProperty(IGNITE_CONFIG_XML, props, null);
            prop.userLibs = getStringProperty(IGNITE_USERS_LIBS, props, null);

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
}
