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

package org.apache.ignite.spi.discovery.tcp;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.continuous.StartRoutineAckDiscoveryMessage;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryStatistics;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryHeartbeatMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;
import org.apache.ignite.testframework.GridStringLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.spi.IgnitePortProtocol.UDP;

/**
 * Clone of TcpDiscoverySelfTest but for using CLUSTER_REGION_ID
 */
public class RegionTcpDiscoverySelfTest extends TcpDiscoverySelfTest {
    /** Region counter. */
    final static AtomicLong regionCount = new AtomicLong(0L);

    /** Provides the nodes with the same name in the same region id. */
    final static ConcurrentHashMap<String, Long> regionIds = new ConcurrentHashMap<>();

    /**
     * @throws Exception If fails.
     */
    public RegionTcpDiscoverySelfTest() throws Exception {
        super();
    }

    /** {@inheritDoc} */
    @Override
    protected TcpDiscoverySpi getTcpDiscoverySpi() {
        return new TcpDiscoverySpiWithRegion();
    }

    /** */
    private static class TcpDiscoverySpiWithRegion extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestTcpDiscoverySpi getTestTcpDiscoverySpi() {
        return new TestTcpDiscoverySpiWithRegion();
    }

    /** */
    private static class TestTcpDiscoverySpiWithRegion extends TestTcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestDiscoveryDataDuplicateSpi getTestDiscoveryDataDuplicateSpi() {
        return new TestDiscoveryDataDuplicateSpiWithRegion();
    }

    /** */
    private static class TestDiscoveryDataDuplicateSpiWithRegion extends TestDiscoveryDataDuplicateSpi {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestEventDiscardSpi getTestEventDiscardSpi() {
        return new TestEventDiscardSpiWithRegion();
    }

    /** */
    private static class TestEventDiscardSpiWithRegion extends TestEventDiscardSpi {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestCustomerEventAckSpi getTestCustomerEventAckSpi() {
        return new TestCustomerEventAckSpiWithRegion();
    }

    /** */
    private static class TestCustomerEventAckSpiWithRegion extends TestCustomerEventAckSpi {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestFailedNodesSpi getTestFailedNodesSpi(int failOrder) {
        return new TestFailedNodesSpi(failOrder);
    }

    /** */
    private static class TestFailedNodesSpiWithRegion extends TestFailedNodesSpi {
        /** {@inheritDoc} */
        TestFailedNodesSpiWithRegion(int failOrder) {
            super(failOrder);
        }

        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestCustomEventCoordinatorFailureSpi getTestCustomEventCoordinatorFailureSpi() {
        return new TestCustomEventCoordinatorFailureSpiWithRegion();
    }

    /** */
    private static class TestCustomEventCoordinatorFailureSpiWithRegion extends TestCustomEventCoordinatorFailureSpi {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestCustomEventRaceSpi getTestCustomEventRaceSpi() {
        return new TestCustomEventRaceSpiWithRegion();
    }

    /** */
    private static class TestCustomEventRaceSpiWithRegion extends TestCustomEventRaceSpi {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestMessageWorkerFailureSpi1 getTestMessageWorkerFailureSpi1(int failureMode) {
        return new TestMessageWorkerFailureSpi1WithRegion(failureMode);
    }

    /** */
    private static class TestMessageWorkerFailureSpi1WithRegion extends TestMessageWorkerFailureSpi1 {
        /** */
        public TestMessageWorkerFailureSpi1WithRegion(int failureMode) {
            super(failureMode);
        }

        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected TestMessageWorkerFailureSpi2 getTestMessageWorkerFailureSpi2() {
        return new TestMessageWorkerFailureSpi2WithRegion();
    }

    /** */
    private static class TestMessageWorkerFailureSpi2WithRegion extends TestMessageWorkerFailureSpi2 {
        /** {@inheritDoc} */
        @Override public void setNodeAttributes(Map<String, Object> attrs,
            IgniteProductVersion ver) {
            long region = regionCount.getAndIncrement();
            Long exist = regionIds.putIfAbsent(getName(), region);
            if (exist != null)
                region = exist;
            attrs.putIfAbsent("CLUSTER_REGION_ID", region);
            super.setNodeAttributes(attrs, ver);
        }
    }
}
