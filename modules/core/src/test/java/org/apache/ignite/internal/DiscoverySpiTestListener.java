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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test callback for discovery SPI.
 * <p>
 * Allows block/delay node join and custom event sending.
 */
public class DiscoverySpiTestListener implements IgniteDiscoverySpiInternalListener {
    /** */
    private volatile CountDownLatch joinLatch;

    /** */
    private volatile CountDownLatch reconLatch;

    /** */
    private Set<Class<?>> blockCustomEvtCls;

    /** */
    private final Object mux = new Object();

    /** */
    private List<DiscoverySpiCustomMessage> blockedMsgs = new ArrayList<>();

    /** */
    private volatile DiscoverySpi spi;

    /** */
    private volatile IgniteLogger log;

    /**
     *
     */
    public void startBlockJoin() {
        joinLatch = new CountDownLatch(1);
    }

    /**
     *
     */
    public void startBlockReconnect() {
        reconLatch = new CountDownLatch(1);
    }

    /**
     *
     */
    public void stopBlockJoin() {
        joinLatch.countDown();
    }

    /**
     *
     */
    public void stopBlockRestart() {
        reconLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void beforeJoin(ClusterNode locNode, IgniteLogger log) {
        try {
            CountDownLatch writeLatch0 = joinLatch;

            if (writeLatch0 != null) {
                log.info("Block join");

                U.await(writeLatch0);
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeReconnect(ClusterNode locNode, IgniteLogger log) {
        try {
            CountDownLatch writeLatch0 = reconLatch;

            if (writeLatch0 != null) {
                log.info("Block reconnect");

                U.await(writeLatch0);
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoverySpiCustomMessage msg) {
        this.spi = spi;
        this.log = log;

        synchronized (mux) {
            if (blockCustomEvtCls != null) {
                DiscoveryCustomMessage msg0 = GridTestUtils.getFieldValue(msg, "delegate");

                if (blockCustomEvtCls.contains(msg0.getClass())) {
                    log.info("Block custom message: " + msg0);

                    blockedMsgs.add(msg);

                    mux.notifyAll();

                    return false;
                }
            }
        }

        return true;
    }

    /**
     * @param blockCustomEvtCls Event class to block.
     */
    public void blockCustomEvent(Class<?> cls0, Class<?>... blockCustomEvtCls) {
        synchronized (mux) {
            assert blockedMsgs.isEmpty() : blockedMsgs;

            this.blockCustomEvtCls = new HashSet<>();

            this.blockCustomEvtCls.add(cls0);

            Collections.addAll(this.blockCustomEvtCls, blockCustomEvtCls);
        }
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    public void waitCustomEvent() throws InterruptedException {
        synchronized (mux) {
            while (blockedMsgs.isEmpty())
                mux.wait();
        }
    }

    /**
     *
     */
    public void stopBlockCustomEvents() {
        if (spi == null)
            return;

        List<DiscoverySpiCustomMessage> msgs;

        synchronized (this) {
            msgs = new ArrayList<>(blockedMsgs);

            blockCustomEvtCls = null;

            blockedMsgs.clear();
        }

        for (DiscoverySpiCustomMessage msg : msgs) {
            log.info("Resend blocked message: " + msg);

            spi.sendCustomEvent(msg);
        }
    }
}
