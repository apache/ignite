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

package org.apache.ignite.spi.discovery.zk;

import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.IgniteDiscoverySpiInternalListenerSupport;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Test wrapper over {@link ZookeeperDiscoverySpi} that is compatible with Ignite Test Framework
 * (see {@link #cloneSpiConfiguration()}) and provides extended ability to intercept SPI events.
 */
public class TestZookeeperDiscoverySpi extends ZookeeperDiscoverySpi implements IgniteDiscoverySpiInternalListenerSupport {
    /** */
    private volatile IgniteDiscoverySpiInternalListener internalLsnr;

    /** {@inheritDoc} */
    @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) {
        IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

        if (internalLsnr != null && !internalLsnr.beforeSendCustomEvent(this, log, msg))
            return;

        super.sendCustomEvent(msg);
    }

    /** {@inheritDoc} */
    @Override public void beforeJoinTopology(ClusterNode locNode) {
        IgniteDiscoverySpiInternalListener internalLsnr = this.internalLsnr;

        if (internalLsnr != null)
            internalLsnr.beforeJoin(locNode, log);
    }

    /** */
    @Override public void setInternalListener(IgniteDiscoverySpiInternalListener lsnr) {
        internalLsnr = lsnr;
    }

    /**
     * Creates a copy of the current SPI instance. Called by Test Framework to run nodes across multiple JVMs.
     * This method is called using the Java Reflection API (see {@link GridAbstractTest#startRemoteGrid}).
     *
     * @return Copy of current SPI instance.
     */
    public ZookeeperDiscoverySpi cloneSpiConfiguration() {
        ZookeeperDiscoverySpi spi = new TestZookeeperDiscoverySpi();

        spi.setZkRootPath(getZkRootPath());
        spi.setZkConnectionString(getZkConnectionString());
        spi.setSessionTimeout(getSessionTimeout());
        spi.setJoinTimeout(getJoinTimeout());
        spi.setClientReconnectDisabled(isClientReconnectDisabled());

        return spi;
    }
}
