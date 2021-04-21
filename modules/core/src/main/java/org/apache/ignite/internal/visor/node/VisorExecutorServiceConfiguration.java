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

package org.apache.ignite.internal.visor.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Data transfer object for node executors configuration properties.
 */
public class VisorExecutorServiceConfiguration extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Public pool size. */
    private int pubPoolSize;

    /** System pool size. */
    private int sysPoolSz;

    /** Management pool size. */
    private int mgmtPoolSize;

    /** IGFS pool size. */
    private int igfsPoolSize;

    /** Peer-to-peer pool size. */
    private int p2pPoolSz;

    /** Rebalance thread pool size. */
    private int rebalanceThreadPoolSize;

    /** REST requests pool size. */
    private int restPoolSz;

    /** Async Callback pool size. */
    private int cbPoolSize;

    /** Data stream pool size. */
    private int dataStreamerPoolSize;

    /** Query pool size. */
    private int qryPoolSize;

    /** Use striped pool for internal requests processing when possible */
    private int stripedPoolSize;

    /** Service pool size. */
    private int svcPoolSize;

    /** Utility cache pool size. */
    private int utilityCachePoolSize;

    /** Client connector configuration pool size. */
    private int cliConnCfgPoolSize;

    /** List of executor configurations. */
    private List<VisorExecutorConfiguration> executors;

    /**
     * Default constructor.
     */
    public VisorExecutorServiceConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for node executors configuration properties.
     *
     * @param c Grid configuration.
     */
    public VisorExecutorServiceConfiguration(IgniteConfiguration c) {
        pubPoolSize = c.getPublicThreadPoolSize();
        sysPoolSz = c.getSystemThreadPoolSize();
        mgmtPoolSize = c.getManagementThreadPoolSize();
        p2pPoolSz = c.getPeerClassLoadingThreadPoolSize();
        rebalanceThreadPoolSize = c.getRebalanceThreadPoolSize();

        ConnectorConfiguration cc = c.getConnectorConfiguration();

        if (cc != null)
            restPoolSz = cc.getThreadPoolSize();

        cbPoolSize = c.getAsyncCallbackPoolSize();
        dataStreamerPoolSize = c.getDataStreamerThreadPoolSize();
        qryPoolSize = c.getQueryThreadPoolSize();
        stripedPoolSize = c.getStripedPoolSize();
        svcPoolSize = c.getServiceThreadPoolSize();
        utilityCachePoolSize = c.getUtilityCacheThreadPoolSize();

        ClientConnectorConfiguration scc = c.getClientConnectorConfiguration();

        if (scc != null)
            cliConnCfgPoolSize = scc.getThreadPoolSize();

        executors = VisorExecutorConfiguration.list(c.getExecutorConfiguration());
    }

    /**
     * @return Public pool size.
     */
    public int getPublicThreadPoolSize() {
        return pubPoolSize;
    }

    /**
     * @return System pool size.
     */
    public int getSystemThreadPoolSize() {
        return sysPoolSz;
    }

    /**
     * @return Management pool size.
     */
    public int getManagementThreadPoolSize() {
        return mgmtPoolSize;
    }

    /**
     * @return IGFS pool size.
     */
    public int getIgfsThreadPoolSize() {
        return igfsPoolSize;
    }

    /**
     * @return Peer-to-peer pool size.
     */
    public int getPeerClassLoadingThreadPoolSize() {
        return p2pPoolSz;
    }

    /**
     * @return Rebalance thread pool size.
     */
    public int getRebalanceThreadPoolSize() {
        return rebalanceThreadPoolSize;
    }

    /**
     * @return REST requests pool size.
     */
    public int getRestThreadPoolSize() {
        return restPoolSz;
    }

    /**
     * @return Thread pool size to be used for processing of asynchronous callbacks.
     */
    public int getCallbackPoolSize() {
        return cbPoolSize;
    }

    /**
     * @return Thread pool size to be used for data stream messages.
     */
    public int getDataStreamerPoolSize() {
        return dataStreamerPoolSize;
    }

    /**
     * @return Thread pool size to be used in grid for query messages.
     */
    public int getQueryThreadPoolSize() {
        return qryPoolSize;
    }

    /**
     * @return The number of threads (stripes) to be used for requests processing.
     */
    public int getStripedPoolSize() {
        return stripedPoolSize;
    }

    /**
     * @return Thread pool size to be used in grid to process service proxy invocations.
     */
    public int getServiceThreadPoolSize() {
        return svcPoolSize;
    }

    /**
     * @return Thread pool size to be used in grid for utility cache messages.
     */
    public int getUtilityCacheThreadPoolSize() {
        return utilityCachePoolSize;
    }

    /**
     * @return Thread pool that is in charge of processing ODBC tasks.
     */
    public int getClientConnectorConfigurationThreadPoolSize() {
        return cliConnCfgPoolSize;
    }

    /**
     * @return List of executor configurations.
     */
    public List<VisorExecutorConfiguration> getExecutors() {
        return executors;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(pubPoolSize);
        out.writeInt(sysPoolSz);
        out.writeInt(mgmtPoolSize);
        out.writeInt(igfsPoolSize);
        out.writeInt(p2pPoolSz);
        out.writeInt(rebalanceThreadPoolSize);
        out.writeInt(restPoolSz);
        out.writeInt(cbPoolSize);
        out.writeInt(dataStreamerPoolSize);
        out.writeInt(qryPoolSize);
        out.writeInt(stripedPoolSize);
        out.writeInt(svcPoolSize);
        out.writeInt(utilityCachePoolSize);
        out.writeInt(cliConnCfgPoolSize);
        U.writeCollection(out, executors);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        pubPoolSize = in.readInt();
        sysPoolSz = in.readInt();
        mgmtPoolSize = in.readInt();
        igfsPoolSize = in.readInt();
        p2pPoolSz = in.readInt();
        rebalanceThreadPoolSize = in.readInt();
        restPoolSz = in.readInt();
        cbPoolSize = in.readInt();
        dataStreamerPoolSize = in.readInt();
        qryPoolSize = in.readInt();
        stripedPoolSize = in.readInt();
        svcPoolSize = in.readInt();
        utilityCachePoolSize = in.readInt();
        cliConnCfgPoolSize = in.readInt();
        executors = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorExecutorServiceConfiguration.class, this);
    }
}
