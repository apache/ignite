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

package org.apache.ignite.internal.processors.security.events;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.security.SecuritySubject;

import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;

/**
 * Abstract class to test correctness of {@link CacheEvent#subjectId}.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractSecurityCacheEventTest extends AbstractSecurityTest {
    /** Counter. */
    protected static final AtomicInteger COUNTER = new AtomicInteger();

    /** Node that registers event listeners. */
    private static final String LISTENER_NODE = "listener_node";

    /** Client node. */
    static final String CLNT = "client";

    /** Server node. */
    static final String SRV = "server";

    /** Events latch. */
    private static CountDownLatch evtsLatch;

    /** */
    private static final AtomicInteger rmtLoginCnt = new AtomicInteger();

    /** */
    private static final AtomicInteger locLoginCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridAllowAll(LISTENER_NODE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setIncludeEventTypes(EventType.EVTS_CACHE_LIFECYCLE);
    }

    /** */
    protected void testCacheEvents(int expTimes, String expLogin, int evtType, Collection<CacheConfiguration> ccfgs,
        Consumer<Collection<CacheConfiguration>> op) throws Exception {

        locLoginCnt.set(0);
        rmtLoginCnt.set(0);

        // For the EVT_CACHE_STOPPED event count of local listener should be 1 due to IGNITE-13010.
        evtsLatch = new CountDownLatch(expTimes + (evtType == EVT_CACHE_STOPPED ? 1 : expTimes));

        UUID lsnrId = grid(LISTENER_NODE).events().remoteListen(new IgniteBiPredicate<UUID, Event>() {
            @Override public boolean apply(UUID uuid, Event evt) {
                if (onEvent((CacheEvent)evt, ccfgs, expLogin))
                    locLoginCnt.incrementAndGet();

                return true;
            }
        }, new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                if (onEvent((CacheEvent)evt, ccfgs, expLogin))
                    rmtLoginCnt.incrementAndGet();

                return true;
            }
        }, evtType);

        try {
            // Execute tested operation.
            op.accept(ccfgs);

            // Waiting for events.
            evtsLatch.await(10, TimeUnit.SECONDS);

            assertEquals("Remote filter.", expTimes, rmtLoginCnt.get());
            // For the EVT_CACHE_STOPPED event expected times of calling local listener should be 0 (ignored)
            // due to IGNITE-13010.
            if (evtType != EVT_CACHE_STOPPED)
                assertEquals("Local listener.", expTimes, locLoginCnt.get());
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListenAsync(lsnrId).get();
        }
    }

    /** */
    protected Collection<CacheConfiguration> cacheConfigurations(int num, boolean needCreateCaches) {
        return cacheConfigurations(num, needCreateCaches, LISTENER_NODE);
    }

    /** */
    protected Collection<CacheConfiguration> cacheConfigurations(int num, boolean needCreateCaches, String node) {
        Collection<CacheConfiguration> res = new ArrayList<>(num);

        for (int i = 0; i < num; i++)
            res.add(new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet()));

        if (needCreateCaches)
            res.forEach(c -> grid(node).createCache(c.getName()));

        return res;
    }

    public static boolean onEvent(CacheEvent evt, Collection<CacheConfiguration> ccfgs, String expLogin) {
        if (ccfgs.stream().noneMatch(ccfg -> ccfg.getName().equals(evt.cacheName())))
            return false;

        evtsLatch.countDown();

        try {
            SecuritySubject subj = IgnitionEx.localIgnite().context().security()
                .authenticatedSubject(evt.subjectId());

            String login = subj.login().toString();

            assertEquals(expLogin, login);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return true;
    }
}
