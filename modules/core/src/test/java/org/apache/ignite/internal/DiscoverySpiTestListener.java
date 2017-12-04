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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.managers.discovery.IgniteDiscoverySpiInternalListener;
import org.apache.ignite.internal.processors.cache.CacheAffinityChangeMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class DiscoverySpiTestListener implements IgniteDiscoverySpiInternalListener {
    /** */
    private volatile CountDownLatch joinLatch;

    /** */
    private boolean blockCustomEvt;

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
    public void startBlock() {
        joinLatch = new CountDownLatch(1);
    }

    /**
     *
     */
    public void stopBlock() {
        joinLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void beforeJoin(IgniteLogger log) {
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
    @Override public boolean beforeSendCustomEvent(DiscoverySpi spi, IgniteLogger log, DiscoverySpiCustomMessage msg) {
        this.spi = spi;
        this.log = log;

        synchronized (mux) {
            if (blockCustomEvt) {
                DiscoveryCustomMessage msg0 = GridTestUtils.getFieldValue(msg, "delegate");

                if (msg0 instanceof CacheAffinityChangeMessage) {
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
     *
     */
    public void blockCustomEvent() {
        synchronized (mux) {
            assert blockedMsgs.isEmpty() : blockedMsgs;

            blockCustomEvt = true;
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

            blockCustomEvt = false;

            blockedMsgs.clear();
        }

        for (DiscoverySpiCustomMessage msg : msgs) {
            log.info("Resend blocked message: " + msg);

            spi.sendCustomEvent(msg);
        }
    }
}
