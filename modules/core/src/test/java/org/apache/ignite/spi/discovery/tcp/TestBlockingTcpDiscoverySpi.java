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

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.jspecify.annotations.NonNull;

/** */
public class TestBlockingTcpDiscoverySpi extends TestTcpDiscoverySpi {
    /** */
    private final BlockingDeque<TcpDiscoveryAbstractMessage> msgQueue = new TestBlockingLinkedDeque();

    /** */
    private volatile boolean isBlocked;

    /** */
    private volatile Predicate<TcpDiscoveryAbstractMessage> msgFilter;
    
    /** */
    private ServerImpl.RingMessageWorker messageWorker;

    /** */
    public TestBlockingTcpDiscoverySpi(TcpDiscoveryIpFinder ipFinder) {
        setIpFinder(ipFinder);
    }

    /** */
    public TestBlockingTcpDiscoverySpi(IgniteConfiguration cfg) {
        this(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());
    }

    /** {@inheritDoc} */
    @Override TcpDiscoveryImpl createServerTcpDiscoveryImplementation() {
        return new ServerImpl(this, DFLT_UTLITY_POOL_SIZE, DFLT_RMT_DC_PING_POOL_SIZE) {
            @Override protected ServerImpl.RingMessageWorker createMessageWorker() {
                messageWorker = new RingMessageWorker(log, msgQueue);

                return messageWorker;
            }
        };
    }

    /** */
    public long pollingTimeout() {
        return messageWorker.pollingTimeout;
    }

    /** */
    public void block() {
        isBlocked = true;
    }

    /** */
    public void unblock() {
        isBlocked = false;
    }

    /** */
    public void messageFilter(Predicate<TcpDiscoveryAbstractMessage> msgFilter) {
        this.msgFilter = msgFilter;
    }

    /** */
    public BlockingDeque<TcpDiscoveryAbstractMessage> messageQueue() {
        return msgQueue;
    }

    /** */
    public static TestBlockingTcpDiscoverySpi blockingDiscovery(Ignite node) {
        return (TestBlockingTcpDiscoverySpi)node.configuration().getDiscoverySpi();
    }

    /** */
    private class TestBlockingLinkedDeque extends LinkedBlockingDeque<TcpDiscoveryAbstractMessage> {
        /** {@inheritDoc} */
        @Override public TcpDiscoveryAbstractMessage poll(long timeout, TimeUnit unit) throws InterruptedException {
            if (isBlocked)
                return null;

            TcpDiscoveryAbstractMessage msg = super.poll(timeout, unit);

            if (isBlocked && msg != null) {
                // Since this queue is processed by a single thread, it is safe to simply put the element back.
                super.addFirst(msg);

                return null;
            }

            return msg;
        }

        /** {@inheritDoc} */
        @Override public void addFirst(@NonNull TcpDiscoveryAbstractMessage t) {
            if (msgFilter != null && msgFilter.test(t))
                return;

            super.addFirst(t);
        }

        /** {@inheritDoc} */
        @Override public boolean add(@NonNull TcpDiscoveryAbstractMessage t) {
            if (msgFilter != null && msgFilter.test(t))
                return true;

            return super.add(t);
        }
    }
}
