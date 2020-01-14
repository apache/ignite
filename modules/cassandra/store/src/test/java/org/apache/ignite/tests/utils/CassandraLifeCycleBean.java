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

package org.apache.ignite.tests.utils;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.LoggerResource;

/**
 * Implementation of {@link LifecycleBean} to start embedded Cassandra instance on Ignite cluster startup
 */
public class CassandraLifeCycleBean implements LifecycleBean {
    /** System property specifying Cassandra jmx port */
    private static final String CASSANDRA_JMX_PORT_PROP = "cassandra.jmx.local.port";

    /** System property specifying Cassandra YAML config file */
    private static final String CASSANDRA_CONFIG_PROP = "cassandra.config";

    /** Prefix for file path syntax */
    private static final String FILE_PREFIX = "file:///";

    /** Auto-injected logger instance. */
    @LoggerResource
    private IgniteLogger log;

    /** Instance of embedded Cassandra database */
    private CassandraDaemon embeddedCassandraDaemon;

    /** JMX port for embedded Cassandra instance */
    private String jmxPort;

    /** YAML config file for embedded Cassandra */
    private String cassandraCfgFile;

    /**
     * Returns JMX port for embedded Cassandra
     * @return JMX port
     */
    public String getJmxPort() {
        return jmxPort;
    }

    /**
     * Setter for embedded Cassandra JMX port
     * @param jmxPort embedded Cassandra JMX port
     */
    public void setJmxPort(String jmxPort) {
        this.jmxPort = jmxPort;
    }

    /**
     * Returns embedded Cassandra YAML config file
     * @return YAML config file
     */
    public String getCassandraConfigFile() {
        return cassandraCfgFile;
    }

    /**
     * Setter for embedded Cassandra YAML config file
     * @param cassandraCfgFile YAML config file
     */
    public void setCassandraConfigFile(String cassandraCfgFile) {
        this.cassandraCfgFile = cassandraCfgFile;
    }

    /** {@inheritDoc} */
    @Override public void onLifecycleEvent(LifecycleEventType evt) {
        if (evt == LifecycleEventType.BEFORE_NODE_START)
            startEmbeddedCassandra();
        else if (evt == LifecycleEventType.BEFORE_NODE_STOP)
            stopEmbeddedCassandra();
    }

    /**
     * Starts embedded Cassandra instance
     */
    private void startEmbeddedCassandra() {
        if (log != null) {
            log.info("-------------------------------");
            log.info("| Starting embedded Cassandra |");
            log.info("-------------------------------");
        }

        try {
            if (jmxPort != null)
                System.setProperty(CASSANDRA_JMX_PORT_PROP, jmxPort);

            if (cassandraCfgFile != null)
                System.setProperty(CASSANDRA_CONFIG_PROP, FILE_PREFIX + cassandraCfgFile);

            embeddedCassandraDaemon = new CassandraDaemon(true);
            embeddedCassandraDaemon.applyConfig();
            embeddedCassandraDaemon.init(null);
            embeddedCassandraDaemon.start();
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to start embedded Cassandra", e);
        }

        if (log != null) {
            log.info("------------------------------");
            log.info("| Embedded Cassandra started |");
            log.info("------------------------------");
        }
    }

    /**
     * Stops embedded Cassandra instance
     */
    private void stopEmbeddedCassandra() {
        if (log != null) {
            log.info("-------------------------------");
            log.info("| Stopping embedded Cassandra |");
            log.info("-------------------------------");
        }

        if (embeddedCassandraDaemon != null) {
            try {
                embeddedCassandraDaemon.deactivate();
            }
            catch (Throwable e) {
                throw new RuntimeException("Failed to stop embedded Cassandra", e);
            }
        }

        if (log != null) {
            log.info("------------------------------");
            log.info("| Embedded Cassandra stopped |");
            log.info("------------------------------");
        }
    }
}
