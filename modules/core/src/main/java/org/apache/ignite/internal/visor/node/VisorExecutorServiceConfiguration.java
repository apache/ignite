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
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;
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
        igfsPoolSize = c.getIgfsThreadPoolSize();
        rebalanceThreadPoolSize = c.getRebalanceThreadPoolSize();

        ConnectorConfiguration cc = c.getConnectorConfiguration();

        if (cc != null)
            restPoolSz = cc.getThreadPoolSize();
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

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(pubPoolSize);
        out.writeInt(sysPoolSz);
        out.writeInt(mgmtPoolSize);
        out.writeInt(igfsPoolSize);
        out.writeInt(p2pPoolSz);
        out.writeInt(rebalanceThreadPoolSize);
        out.writeInt(restPoolSz);
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
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorExecutorServiceConfiguration.class, this);
    }
}
