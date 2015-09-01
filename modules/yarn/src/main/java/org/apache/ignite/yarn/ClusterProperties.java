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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Cluster settings.
 */
public class ClusterProperties {
    /** */
    private static final Logger log = Logger.getLogger(ClusterProperties.class.getSimpleName());

    /** */
    public static final String EMPTY_STRING = "";

    /** */
    public static final String IGNITE_CLUSTER_NAME = "IGNITE_CLUSTER_NAME";

    /** */
    public static final String DEFAULT_CLUSTER_NAME = "ignite-cluster";

    /** */
    public static final double DEFAULT_CPU_PER_NODE = 2;

    /** */
    public static final double DEFAULT_MEM_PER_NODE = 2048;

    /** Cluster name. */
    private String clusterName = DEFAULT_CLUSTER_NAME;

    /** */
    public static final String IGNITE_RUN_CPU_PER_NODE = "IGNITE_RUN_CPU_PER_NODE";

    /** CPU limit. */
    private double cpuPerNode = DEFAULT_CPU_PER_NODE;

    /** */
    public static final String IGNITE_MEMORY_PER_NODE = "IGNITE_MEMORY_PER_NODE";

    /** Memory limit. */
    private double memPerNode = DEFAULT_MEM_PER_NODE;

    /** */
    public static final String IGNITE_NODE_COUNT = "IGNITE_NODE_COUNT";

    /** */
    public static final double DEFAULT_IGNITE_NODE_COUNT = 3;

    /** Node count limit. */
    private double nodeCnt = DEFAULT_IGNITE_NODE_COUNT;

    /** */
    public static final String IGNITE_URL = "IGNITE_URL";

    /** Ignite version. */
    private String igniteUrl = null;

    /** */
    public static final String IGNITE_WORKING_DIR = "IGNITE_WORKING_DIR";

    /** */
    public static final String DEFAULT_IGNITE_WORK_DIR = "/ignite/workdir/";

    /** Ignite work directory. */
    private String igniteWorkDir = DEFAULT_IGNITE_WORK_DIR;

    /** */
    public static final String IGNITE_PATH = "IGNITE_PATH";

    /** Ignite path. */
    private String ignitePath = null;

    /** */
    public static final String LICENCE_PATH = "LICENCE_PATH";

    /** Licence path. */
    private String licencePath = null;

    /** */
    public static final String IGNITE_JVM_OPTS = "IGNITE_JVM_OPTS";

    /** JVM opts. */
    private String jvmOpts = null;

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
    public static final String IGNITE_CONFIG_XML = "IGNITE_XML_CONFIG";

    /** Ignite config. */
    private String igniteCfg = null;

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
     * Sets instance count limit.
     */
    public void instances(int nodeCnt) {
        this.nodeCnt = nodeCnt;
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
     * @return Ignite version.
     */
    public String igniteUrl() {
        return igniteUrl;
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
     * @return Licence path.
     */
    public String licencePath() {
        return licencePath;
    }

    /**
     * @return Ignite hdfs path.
     */
    public String ignitePath() {
        return ignitePath;
    }

    /**
     * @return Jvm opts.
     */
    public String jvmOpts() {
        return jvmOpts;
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

            prop.cpuPerNode = getDoubleProperty(IGNITE_RUN_CPU_PER_NODE, props, DEFAULT_CPU_PER_NODE);
            prop.memPerNode = getDoubleProperty(IGNITE_MEMORY_PER_NODE, props, DEFAULT_MEM_PER_NODE);
            prop.nodeCnt = getDoubleProperty(IGNITE_NODE_COUNT, props, DEFAULT_IGNITE_NODE_COUNT);

            prop.igniteUrl = getStringProperty(IGNITE_URL, props, null);
            prop.ignitePath = getStringProperty(IGNITE_PATH, props, null);
            prop.licencePath = getStringProperty(LICENCE_PATH, props, null);
            prop.jvmOpts = getStringProperty(IGNITE_JVM_OPTS, props, null);
            prop.igniteWorkDir = getStringProperty(IGNITE_WORKING_DIR, props, DEFAULT_IGNITE_WORK_DIR);
            prop.igniteLocalWorkDir = getStringProperty(IGNITE_LOCAL_WORK_DIR, props, DEFAULT_IGNITE_LOCAL_WORK_DIR);
            prop.igniteReleasesDir = getStringProperty(IGNITE_RELEASES_DIR, props, DEFAULT_IGNITE_RELEASES_DIR);
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
     * @return Cluster configuration.
     */
    public static ClusterProperties from() {
        ClusterProperties prop = new ClusterProperties();

        prop.clusterName = getStringProperty(IGNITE_CLUSTER_NAME, null, DEFAULT_CLUSTER_NAME);

        prop.cpuPerNode = getDoubleProperty(IGNITE_RUN_CPU_PER_NODE, null, DEFAULT_CPU_PER_NODE);
        prop.memPerNode = getDoubleProperty(IGNITE_MEMORY_PER_NODE, null, DEFAULT_MEM_PER_NODE);
        prop.nodeCnt = getDoubleProperty(IGNITE_NODE_COUNT, null, DEFAULT_IGNITE_NODE_COUNT);

        prop.igniteUrl = getStringProperty(IGNITE_URL, null, null);
        prop.ignitePath = getStringProperty(IGNITE_PATH, null, null);
        prop.licencePath = getStringProperty(LICENCE_PATH, null, null);
        prop.jvmOpts = getStringProperty(IGNITE_JVM_OPTS, null, null);
        prop.igniteWorkDir = getStringProperty(IGNITE_WORKING_DIR, null, DEFAULT_IGNITE_WORK_DIR);
        prop.igniteLocalWorkDir = getStringProperty(IGNITE_LOCAL_WORK_DIR, null, DEFAULT_IGNITE_LOCAL_WORK_DIR);
        prop.igniteReleasesDir = getStringProperty(IGNITE_RELEASES_DIR, null, DEFAULT_IGNITE_RELEASES_DIR);
        prop.igniteCfg = getStringProperty(IGNITE_CONFIG_XML, null, null);
        prop.userLibs = getStringProperty(IGNITE_USERS_LIBS, null, null);

        String pattern = getStringProperty(IGNITE_HOSTNAME_CONSTRAINT, null, null);

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

    /**
     * Convert to properties to map.
     *
     * @return Key-value map.
     */
    public Map<String, String> toEnvs() {
        Map<String, String> envs = new HashMap<>();

        envs.put(IGNITE_CLUSTER_NAME, toEnvVal(clusterName));

        envs.put(IGNITE_RUN_CPU_PER_NODE, toEnvVal(cpuPerNode));
        envs.put(IGNITE_MEMORY_PER_NODE, toEnvVal(memPerNode));
        envs.put(IGNITE_NODE_COUNT, toEnvVal(nodeCnt));

        envs.put(IGNITE_URL, toEnvVal(igniteUrl));
        envs.put(IGNITE_PATH, toEnvVal(ignitePath));
        envs.put(LICENCE_PATH, toEnvVal(licencePath));
        envs.put(IGNITE_JVM_OPTS, toEnvVal(jvmOpts));
        envs.put(IGNITE_WORKING_DIR, toEnvVal(igniteWorkDir));
        envs.put(IGNITE_LOCAL_WORK_DIR, toEnvVal(igniteLocalWorkDir));
        envs.put(IGNITE_RELEASES_DIR, toEnvVal(igniteReleasesDir));
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
     * @return If val is null {@code EMPTY_STRING} else to string.
     */
    private String toEnvVal(Object val) {
        return val == null ? EMPTY_STRING : val.toString();
    }
}