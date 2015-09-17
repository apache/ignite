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

import java.io.Serializable;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for node executors configuration properties.
 */
public class VisorExecutorServiceConfiguration implements Serializable {
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

    /** REST requests pool size. */
    private int restPoolSz;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node executors configuration properties.
     */
    public static VisorExecutorServiceConfiguration from(IgniteConfiguration c) {
        VisorExecutorServiceConfiguration cfg = new VisorExecutorServiceConfiguration();

        cfg.pubPoolSize = c.getPublicThreadPoolSize();
        cfg.sysPoolSz = c.getSystemThreadPoolSize();
        cfg.mgmtPoolSize = c.getManagementThreadPoolSize();
        cfg.p2pPoolSz = c.getPeerClassLoadingThreadPoolSize();
        cfg.igfsPoolSize = c.getIgfsThreadPoolSize();

        ConnectorConfiguration cc = c.getConnectorConfiguration();

        if (cc != null)
            cfg.restPoolSz = cc.getThreadPoolSize();

        return cfg;
    }

    /**
     * @return Public pool size.
     */
    public int publicThreadPoolSize() {
        return pubPoolSize;
    }

    /**
     * @return System pool size.
     */
    public int systemThreadPoolSize() {
        return sysPoolSz;
    }

    /**
     * @return Management pool size.
     */
    public int managementThreadPoolSize() {
        return mgmtPoolSize;
    }

    /**
     * @return IGFS pool size.
     */
    public int igfsThreadPoolSize() {
        return igfsPoolSize;
    }

    /**
     * @return Peer-to-peer pool size.
     */
    public int peerClassLoadingThreadPoolSize() {
        return p2pPoolSz;
    }

    /**
     * @return REST requests pool size.
     */
    public int restThreadPoolSize() {
        return restPoolSz;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorExecutorServiceConfiguration.class, this);
    }
}