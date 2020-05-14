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

    /** Remote filter latch. */
    private static volatile CountDownLatch rmtLatch;

    /** Local listener latch. */
    private static volatile CountDownLatch locLatch;

    /** Set that registers security subjects on remote nodes. */
    private static final Collection<String> rmtSubjLogin = new ConcurrentLinkedQueue<>();

    /** Set that registers security subjects on local node. */
    private static final Collection<String> locSubjLogin = new ConcurrentLinkedQueue<>();

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
    private void prepareTest(int expTimes, boolean isDestroy) {
        rmtSubjLogin.clear();

        locSubjLogin.clear();

        rmtLatch = new CountDownLatch(expTimes);

        locLatch = new CountDownLatch(isDestroy ? 1 : expTimes);
    }

    /** */
    protected void testCacheEvents(int expTimes, String expSubj, int evtType, Collection<CacheConfiguration> ccfgs,
        Consumer<Collection<CacheConfiguration>> op) throws Exception {
        boolean isDestroy = evtType == EVT_CACHE_STOPPED;

        prepareTest(expTimes, isDestroy);

        if (isDestroy)
            grid(LISTENER_NODE).createCaches(ccfgs);

        Set<String> cacheNames = cacheNames(ccfgs);

        UUID lsnrId = grid(LISTENER_NODE)
            .events()
            .remoteListen(localListener(cacheNames), remoteFilter(cacheNames), evtType);

        try {
            op.accept(ccfgs);

            rmtLatch.await(10, TimeUnit.SECONDS);
            locLatch.await(10, TimeUnit.SECONDS);

            checkSubject(rmtSubjLogin, expTimes, expSubj, "Remote filter.");

            int locExpTimes = isDestroy ? 0 : expTimes;

            checkSubject(locSubjLogin, locExpTimes, expSubj, "Local listener.");
        }
        finally {
            grid(LISTENER_NODE).events().stopRemoteListen(lsnrId);
        }
    }

    /** */
    protected Set<String> cacheNames(Collection<CacheConfiguration> ccfgs) {
        return ccfgs.stream().map(CacheConfiguration::getName).collect(Collectors.toSet());
    }

    /** */
    protected Collection<CacheConfiguration> cacheConfigurations(int num) {
        if (num == 1)
            return Collections.singletonList(new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet()));

        Collection<CacheConfiguration> res = new ArrayList<>(num);

        for (int i = 0; i < num; i++)
            res.add(new CacheConfiguration("test_cache_" + COUNTER.incrementAndGet()));

        return res;
    }

    /** */
    private void checkSubject(Collection<String> subjects, int expTimes, String expSubj, String msgPrefix) {
        Set<String> set = new HashSet<>(subjects);

        if (set.size() != 1) {
            fail(msgPrefix + " Expected subject: " + expSubj +
                ". Actual subjects: " + Iterators.toString(set.iterator()));
        }
        else
            assertEquals(msgPrefix, expSubj, subjects.iterator().next());

        if (expTimes > 0)
            assertEquals(msgPrefix, expTimes, subjects.size());
    }

    /** */
    private IgnitePredicate<Event> remoteFilter(final Set<String> cacheNames) {
        return new IgnitePredicate<Event>() {
            /** */
            @Override public boolean apply(Event evt) {
                try {
                    CacheEvent cacheEvt = (CacheEvent)evt;

                    if (cacheNames.contains(cacheEvt.cacheName())) {
                        SecuritySubject subj = IgnitionEx.localIgnite().context().security()
                            .authenticatedSubject(cacheEvt.subjectId());

                        rmtSubjLogin.add(subj.login().toString());

                        if (rmtLatch != null)
                            rmtLatch.countDown();
                    }

                    return true;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }

    /** */
    private IgniteBiPredicate<UUID, Event> localListener(final Set<String> cacheNames) {
        return new IgniteBiPredicate<UUID, Event>() {
            /** */
            @Override public boolean apply(UUID uuid, Event evt) {
                try {
                    CacheEvent cacheEvt = (CacheEvent)evt;

                    if (cacheNames.contains(cacheEvt.cacheName())) {

                        SecuritySubject subj = IgnitionEx.localIgnite().context().security()
                            .authenticatedSubject(cacheEvt.subjectId());

                        locSubjLogin.add(subj.login().toString());

                        if (locLatch != null)
                            locLatch.countDown();
                    }

                    return true;
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        };
    }
}
