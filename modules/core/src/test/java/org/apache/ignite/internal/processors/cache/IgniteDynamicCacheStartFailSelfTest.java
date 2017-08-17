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

package org.apache.ignite.internal.processors.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

@SuppressWarnings("unchecked")
public class IgniteDynamicCacheStartFailSelfTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String DYNAMIC_CACHE_NAME = "TestDynamicCache";

    /**
     * @return Number of nodes for this test.
     */
    public int nodeCount() {
        return 2;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        cfg.setIncludeEventTypes(EventType.EVT_CACHE_STARTED, EventType.EVT_CACHE_STOPPED, EventType.EVT_CACHE_NODES_LEFT);

        return cfg;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(nodeCount());
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**  */
    public void testGetOrCreateCollectionExceptional() throws Exception {
        final IgniteEx grid = grid(0);

        for (int failIdx = 0; failIdx < nodeCount(); ++failIdx) {
            final CacheConfiguration cfg = new CacheConfiguration();

            cfg.setName(DYNAMIC_CACHE_NAME + "-" + Integer.toString(failIdx));

            cfg.setAffinity(new ExceptionalAffinityFunction(false, getTestGridName(failIdx)));

            try {
                grid.getOrCreateCache(cfg);
                fail("Simulated exception must be thrown. failIdx " + failIdx);
            }
            catch (CacheException e) {
                log().info(e.toString());
            }
        }
    }

    private static class ExceptionalAffinityFunction extends RendezvousAffinityFunction {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Exception should arise on all nodes. */
        private boolean exceptionOnAllNodes = false;

        /** Exception should arise on node with certain name. */
        private String gridName;

        public ExceptionalAffinityFunction() {
            // No-op.
        }

        public ExceptionalAffinityFunction(boolean exceptionOnAllNodes, String gridName) {
            this.exceptionOnAllNodes = exceptionOnAllNodes;
            this.gridName = gridName;
        }

        /**
         * Throws simulated exception
         * {@inheritDoc}
         */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            if (exceptionOnAllNodes || ignite.name().equals(gridName))
                throw new IllegalStateException("Simulated exception");
            else
                return super.assignPartitions(affCtx);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);
            out.writeBoolean(exceptionOnAllNodes);
            out.writeObject(gridName);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);
            exceptionOnAllNodes = in.readBoolean();
            gridName = (String)in.readObject();
        }
    }
}
