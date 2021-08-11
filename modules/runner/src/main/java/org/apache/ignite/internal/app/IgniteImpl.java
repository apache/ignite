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

package org.apache.ignite.internal.app;

import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.internal.configuration.ConfigurationManager;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.processors.query.calcite.SqlQueryProcessor;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Ignite internal implementation.
 */
public class IgniteImpl implements Ignite {
    /** Distributed table manager. */
    private final IgniteTables distributedTblMgr;

    /** Ignite node name. */
    private final String name;

    private final SqlQueryProcessor qryEngine;

    /** Configuration manager that handles node (local) configuration. */
    private final ConfigurationManager nodeConfigurationMgr;

    /** Configuration manager that handles cluster (distributed) configuration. */
    private final ConfigurationManager clusterConfigurationMgr;

    /**
     * @param name Ignite node name.
     * @param tblMgr Table manager.
     * @param qryEngine Query processor.
     * @param nodeConfigurationMgr Configuration manager that handles node (local) configuration.
     * @param clusterConfigurationMgr Configuration manager that handles cluster (distributed) configuration.
     */
    IgniteImpl(
        String name,
        IgniteTables tblMgr,
        SqlQueryProcessor qryEngine,
        ConfigurationManager nodeConfigurationMgr,
        ConfigurationManager clusterConfigurationMgr
    ) {
        this.name = name;
        this.distributedTblMgr = tblMgr;
        this.qryEngine = qryEngine;
        this.nodeConfigurationMgr = nodeConfigurationMgr;
        this.clusterConfigurationMgr = clusterConfigurationMgr;
    }

    /** {@inheritDoc} */
    @Override public IgniteTables tables() {
        return distributedTblMgr;
    }

    public SqlQueryProcessor queryEngine() {
        return qryEngine;
    }

    /** {@inheritDoc} */
    @Override public IgniteTransactions transactions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        IgnitionManager.stop(name);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /**
     * @return Node configuration.
     */
    public ConfigurationRegistry nodeConfiguration() {
        return nodeConfigurationMgr.configurationRegistry();
    }

    /**
     * @return Cluster configuration.
     */
    public ConfigurationRegistry clusterConfiguration() {
        return clusterConfigurationMgr.configurationRegistry();
    }
}
