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

package org.apache.ignite.internal.processor.security.messaging;

import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractResolveSecurityContextTest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.GridAbstractTest;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for IgniteMessaging.
 */
public class IgniteMessagingTest extends AbstractResolveSecurityContextTest {
    /** Sever node that has all permissions for TEST_CACHE. */
    private IgniteEx evntAllPerms;

    /** Sever node that hasn't permissions for TEST_CACHE. */
    private IgniteEx evntNotPerms;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        evntAllPerms = startGrid("evnt_all_perms", allowAllPermissionSet());

        evntNotPerms = startGrid("evnt_not_perms",
            builder().defaultAllowAll(true)
                .appendCachePermissions(CACHE_NAME, EMPTY_PERMS).build());

        super.beforeTestsStarted();
    }

    /** Barrier. */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(2);

    /**
     *
     */
    public void testMessaging() throws Exception {
        awaitPartitionMapExchange();

        assertAllowedResult(key -> messaging(clntAllPerms, clntReadOnlyPerm, evntAllPerms, key));
        assertAllowedResult(key -> messaging(clntAllPerms, srvReadOnlyPerm, evntAllPerms, key));
        assertAllowedResult(key -> messaging(srvAllPerms, clntReadOnlyPerm, evntAllPerms, key));
        assertAllowedResult(key -> messaging(srvAllPerms, srvReadOnlyPerm, evntAllPerms, key));

        assertAllowedResult(key -> messaging(clntAllPerms, srvReadOnlyPerm, evntNotPerms, key));
        assertAllowedResult(key -> messaging(clntAllPerms, clntReadOnlyPerm, evntNotPerms, key));
        assertAllowedResult(key -> messaging(srvAllPerms, srvReadOnlyPerm, evntNotPerms, key));
        assertAllowedResult(key -> messaging(srvAllPerms, clntReadOnlyPerm, evntNotPerms, key));

        assertForbiddenResult(key -> messaging(clntReadOnlyPerm, srvAllPerms, evntAllPerms, key));
        assertForbiddenResult(key -> messaging(clntReadOnlyPerm, clntAllPerms, evntAllPerms, key));
        assertForbiddenResult(key -> messaging(srvReadOnlyPerm, srvAllPerms, evntAllPerms, key));
        assertForbiddenResult(key -> messaging(srvReadOnlyPerm, clntAllPerms, evntAllPerms, key));

        assertForbiddenResult(key -> messaging(clntReadOnlyPerm, srvAllPerms, evntNotPerms, key));
        assertForbiddenResult(key -> messaging(clntReadOnlyPerm, clntAllPerms, evntNotPerms, key));
        assertForbiddenResult(key -> messaging(srvReadOnlyPerm, srvAllPerms, evntNotPerms, key));
        assertForbiddenResult(key -> messaging(srvReadOnlyPerm, clntAllPerms, evntNotPerms, key));
    }

    /**
     * @param lsnr Listener node.
     * @param remote Remote node.
     * @param evt Event node.
     * @param key Key.
     */
    private Integer messaging(IgniteEx lsnr, IgniteEx remote, IgniteEx evt, String key) {
        BARRIER.reset();

        IgniteMessaging messaging = lsnr.message(lsnr.cluster().forNode(remote.localNode()));

        Integer val = values.incrementAndGet();

        String topic = "HOT_TOPIC " + val;

        UUID lsnrId = messaging.remoteListen(topic,
            new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    try {
                        Ignition.localIgnite().cache(CACHE_NAME).put(key, val);

                        return true;
                    }
                    finally {
                        barrierAwait();
                    }
                }
            }
        );

        try {
            evt.message(evt.cluster().forNode(remote.localNode())).send(topic, "Fire!");
        }
        finally {
            barrierAwait();

            messaging.stopRemoteListen(lsnrId);
        }

        return val;
    }

    /**
     * Call await method on {@link #BARRIER} with {@link GridAbstractTest#getTestTimeout()} timeout.
     */
    private void barrierAwait() {
        try {
            BARRIER.await(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
            fail(e.toString());
        }
    }

    /**
     * @param f Function.
     */
    private void assertAllowedResult(Function<String, Integer> f) {
        assertResult(f, false);
    }

    /**
     * @param f Function.
     */
    private void assertForbiddenResult(Function<String, Integer> f) {
        assertResult(f, true);
    }

    /**
     * @param f Function.
     * @param failExpected True if expectaed fail behavior.
     */
    private void assertResult(Function<String, Integer> f, boolean failExpected) {
        String key = failExpected ? "fail_key" : "key";

        Integer val = f.apply(key);

        assertThat(srvAllPerms.cache(CACHE_NAME).get(key), failExpected ? nullValue() : is(val));
    }
}
