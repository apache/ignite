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
public class ClusterResources {
    /** Unlimited. */
    public static final int DEFAULT_VALUE = -1;

    /** */
    public static final String IGNITE_RESOURCE_CPU_CORES = "IGNITE_RESOURCE_CPU_CORES";

    /** CPU limit. */
    private double cpu = DEFAULT_VALUE;

    /** */
    public static final String IGNITE_RESOURCE_MEM_MB = "IGNITE_RESOURCE_MEM_MB";

    /** Memory limit. */
    private double mem = DEFAULT_VALUE;

    /** */
    public static final String IGNITE_RESOURCE_DISK_MB = "IGNITE_RESOURCE_DISK_MB";

    /** Disk space limit. */
    private double disk = DEFAULT_VALUE;

    /** */
    public static final String IGNITE_RESOURCE_NODE_CNT = "IGNITE_RESOURCE_NODE_CNT";

    /** Node count limit. */
    private double nodeCnt = DEFAULT_VALUE;

    /** */
    public static final String IGNITE_RESOURCE_MIN_CPU_CNT_PER_NODE = "IGNITE_RESOURCE_MIN_CPU_CNT_PER_NODE";

    /** Min memory per node. */
    private int minCpu = 2;

    /** */
    public static final String IGNITE_RESOURCE_MIN_MEMORY_PER_NODE = "IGNITE_RESOURCE_MIN_MEMORY_PER_NODE";

    /** Min memory per node. */
    private int minMemoryCnt = 256;

    /** */
    public ClusterResources() {
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
    public int minMemoryPerNode() {
        return minMemoryCnt;
    }

    /**
     * @return min cpu count per node.
     */
    public int minCpuPerNode() {
        return minCpu;
    }

    /**
     * @param config path to config file.
     * @return Cluster configuration.
     */
    public static ClusterResources from(String config) {
        try {
            Properties props = null;

            if (config != null) {
                props = new Properties();

                props.load(new FileInputStream(config));
            }

            ClusterResources resources = new ClusterResources();

            resources.cpu = getProperty(IGNITE_RESOURCE_CPU_CORES, props);
            resources.mem = getProperty(IGNITE_RESOURCE_MEM_MB, props);
            resources.disk = getProperty(IGNITE_RESOURCE_DISK_MB, props);
            resources.nodeCnt = getProperty(IGNITE_RESOURCE_NODE_CNT, props);

            return resources;
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
    private static double getProperty(String name, Properties fileProps) {
        if (fileProps != null && fileProps.containsKey(name))
            return Double.valueOf(fileProps.getProperty(name));

        String property = System.getProperty(name);

        if (property == null)
            property = System.getenv(name);

        if (property == null)
            return DEFAULT_VALUE;

        return Double.valueOf(property);
    }
}
