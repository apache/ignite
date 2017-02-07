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

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Multithreaded reduce query tests with lots of data.
 */
public class GridCacheSplitAwareTopologyValidatorSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final String DC_NODE_ATTR = "dc";

    /** */
    private static final String SPLIT_RESOLVED_NODE_ATTR = "split.resolved";

    /** */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    private String dataCenter;

    private TopologyValidator validator;

    private int cnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(getTestGridIndex(gridName));

        cfg.setUserAttributes(F.asMap(DC_NODE_ATTR, getTestGridIndex(gridName)%2));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setTopologyValidator(new SplitAwareTopologyValidator(getTestGridName(0)));

        if (getTestGridIndex(gridName) == 0)
            validator = cfg.getTopologyValidator();

        return cfg;
    }

    /**
     * Tests topology split scenario.
     * @throws Exception
     */
    public void testSplitSimulation() throws Exception {
        putOp();
    }

    /** */
    private void putOp() {
        jcache().put(String.valueOf(cnt), cnt);

        cnt++;

        assertEquals("Size", cnt, jcache().size());
    }

    /**
     * Prevents grid from performing operations if only nodes from single data center are left in topology.
     */
    private static class SplitAwareTopologyValidator implements TopologyValidator, LifecycleAware {
        @IgniteInstanceResource
        private Ignite ignite;

        @LoggerResource
        private IgniteLogger logger;

        private String name;

        /** */
        private ConcurrentMap<Object, Long> markerVersions = new ConcurrentHashMap8<>();

        /** */
        private long lastTopVer;

        /** */
        public SplitAwareTopologyValidator(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            lastTopVer = ignite.cluster().topologyVersion();

            if (ignite.name().equals(name))
                logger.info("Nodes: " + nodes.size() + " topVer: " + lastTopVer);

            boolean hasSplit = true;

            ClusterNode recovered = null;

            ClusterNode prev = null;

            for (ClusterNode node : nodes) {
                if (node.isClient())
                    continue;

                if (prev != null &&
                    !prev.attribute(DC_NODE_ATTR).equals(node.attribute(DC_NODE_ATTR)))
                    hasSplit = false;

                prev = node;

                if (node.attribute(SPLIT_RESOLVED_NODE_ATTR) != null)
                    recovered = node;
            }

            if (hasSplit && recovered != null) {
                logger.error("Segmentation validation detected a marker node, " +
                    "but topology still doesn't contain nodes from other DC. Grid operations are disabled!");

                return false;
            }

            if (recovered != null) {
                Long recVer = markerVersions.get(recovered);

                if (recVer == null)
                    recVer = lastTopVer;

                markerVersions.put(recovered.consistentId(), lastTopVer);

                if (lastTopVer <= recVer)
                    return false;
            }

            return true;
        }

        @Override public void start() throws IgniteException {
            ignite.events().localListen(new IgnitePredicate<Event>() {
                @Override public boolean apply(Event event) {
                    return false;
                }
            }, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED);
        }

        @Override public void stop() throws IgniteException {

        }
    }
}