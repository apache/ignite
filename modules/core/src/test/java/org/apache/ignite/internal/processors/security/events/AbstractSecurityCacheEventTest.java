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

import com.google.common.collect.Iterators;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
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

    /** Events latch. */
    private static volatile CountDownLatch evtsLatch;

    /** Logins in remote filters. */
    private static final Collection<String> rmtLogins = new ConcurrentLinkedQueue<>();

    /** Logins in a local listener. */
    private static final Collection<String> locLogins = new ConcurrentLinkedQueue<>();

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
        // For the EVT_CACHE_STOPPED event count of local listener should be 1 due to IGNITE-13010.
        evtsLatch = new CountDownLatch(expTimes + (evtType == EVT_CACHE_STOPPED ? 1 : expTimes));

        rmtLogins.clear();
        locLogins.clear();

        UUID lsnrId = grid(LISTENER_NODE)
            .events()
            .remoteListen(
                new TestPredicate(ccfgs) {
                    @Override void register(String login) {
                        locLogins.add(login);
                    }
                },
                new TestPredicate(ccfgs) {
                    @Override void register(String login) {
                        rmtLogins.add(login);
                    }
                }, evtType);

        try {
            // Execute tested operation.
            op.accept(ccfgs);

            // Waiting for events.
            evtsLatch.await(10, TimeUnit.SECONDS);

            checkResult(expLogin, rmtLogins, expTimes, "Remote filter.");
            // For the EVT_CACHE_STOPPED event expected times of calling local listener should be 0 (ignored)
            // due to IGNITE-13010.
            checkResult(expLogin, locLogins, evtType == EVT_CACHE_STOPPED ? 0 : expTimes, "Local listener.");
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(lsnrId);
        }
    }

    /** */
    private void checkResult(String expLogin, Collection<String> logins, int expTimes, String msgPrefix) {
        Set<String> set = new HashSet<>(logins);

        if (set.size() != 1) {
            fail(msgPrefix + " Expected subject: " + expLogin +
                ". Actual subjects: " + Iterators.toString(set.iterator()));
        }
        else
            assertEquals(msgPrefix, expLogin, logins.iterator().next());

        if (expTimes > 0)
            assertEquals(msgPrefix, expTimes, logins.size());
    }

    /** */
    protected Collection<CacheConfiguration> cacheConfigurations(int num, boolean needCreateCaches) {
        Collection<CacheConfiguration> res;

        if (num == 1)
            res = Collections.singletonList(new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet()));
        else {
            res = new ArrayList<>(num);

            for (int i = 0; i < num; i++)
                res.add(new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet()));
        }

        if (needCreateCaches)
            res.forEach(c -> grid(LISTENER_NODE).createCache(c.getName()));

        return res;
    }

    /**
     * Remote filter or local listener predicate.
     */
    private abstract static class TestPredicate implements IgniteBiPredicate<UUID, Event>, IgnitePredicate<Event> {
        /** Expected cache names. */
        private final Set<String> cacheNames;

        /** */
        private TestPredicate(Collection<CacheConfiguration> ccfgs) {
            cacheNames = ccfgs.stream().map(CacheConfiguration::getName).collect(Collectors.toSet());
        }

        /** */
        private void body(Event evt) {
            try {
                CacheEvent cacheEvt = (CacheEvent)evt;

                if (cacheNames.contains(cacheEvt.cacheName())) {
                    SecuritySubject subj = IgnitionEx.localIgnite().context().security()
                        .authenticatedSubject(cacheEvt.subjectId());

                    evtsLatch.countDown();

                    register(subj.login().toString());
                }
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** */
        abstract void register(String login);

        /** {@inheritDoc} */
        @Override public boolean apply(UUID uuid, Event evt) {
            body(evt);

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            body(evt);

            return true;
        }
    }
}
