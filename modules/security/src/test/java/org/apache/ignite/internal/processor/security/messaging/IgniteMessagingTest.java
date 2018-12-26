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
import java.util.function.Consumer;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractResolveSecurityContextTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Testing permissions when the message listener is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
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
    @Test
    public void testMessaging() throws Exception {
        awaitPartitionMapExchange();

        assertAllowedResult(t -> messaging(clntAllPerms, clntReadOnlyPerm, evntAllPerms, t));
        assertAllowedResult(t -> messaging(clntAllPerms, srvReadOnlyPerm, evntAllPerms, t));
        assertAllowedResult(t -> messaging(srvAllPerms, clntReadOnlyPerm, evntAllPerms, t));
        assertAllowedResult(t -> messaging(srvAllPerms, srvReadOnlyPerm, evntAllPerms, t));

        assertAllowedResult(t -> messaging(clntAllPerms, srvReadOnlyPerm, evntNotPerms, t));
        assertAllowedResult(t -> messaging(clntAllPerms, clntReadOnlyPerm, evntNotPerms, t));
        assertAllowedResult(t -> messaging(srvAllPerms, srvReadOnlyPerm, evntNotPerms, t));
        assertAllowedResult(t -> messaging(srvAllPerms, clntReadOnlyPerm, evntNotPerms, t));

        assertForbiddenResult(t -> messaging(clntReadOnlyPerm, srvAllPerms, evntAllPerms, t));
        assertForbiddenResult(t -> messaging(clntReadOnlyPerm, clntAllPerms, evntAllPerms, t));
        assertForbiddenResult(t -> messaging(srvReadOnlyPerm, srvAllPerms, evntAllPerms, t));
        assertForbiddenResult(t -> messaging(srvReadOnlyPerm, clntAllPerms, evntAllPerms, t));

        assertForbiddenResult(t -> messaging(clntReadOnlyPerm, srvAllPerms, evntNotPerms, t));
        assertForbiddenResult(t -> messaging(clntReadOnlyPerm, clntAllPerms, evntNotPerms, t));
        assertForbiddenResult(t -> messaging(srvReadOnlyPerm, srvAllPerms, evntNotPerms, t));
        assertForbiddenResult(t -> messaging(srvReadOnlyPerm, clntAllPerms, evntNotPerms, t));
    }

    /**
     * @param lsnr Listener node.
     * @param remote Remote node.
     * @param evt Event node.
     * @param t Entry to put into test cache.
     */
    private void messaging(IgniteEx lsnr, IgniteEx remote, IgniteEx evt, T2<String, Integer> t) {
        BARRIER.reset();

        IgniteMessaging messaging = lsnr.message(lsnr.cluster().forNode(remote.localNode()));

        String topic = "HOT_TOPIC " + t.getKey();

        UUID lsnrId = messaging.remoteListen(topic,
            new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    try {
                        Ignition.localIgnite().cache(CACHE_NAME).put(t.getKey(), t.getValue());

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
     * @param c Consumer.
     */
    private void assertAllowedResult(Consumer<T2<String, Integer>> c) {
        assertResult(c, false);
    }

    /**
     * @param c Consumer.
     */
    private void assertForbiddenResult(Consumer<T2<String, Integer>> c) {
        assertResult(c, true);
    }

    /**
     * @param c Consumer.
     * @param failExp True if expectaed fail behavior.
     */
    private void assertResult(Consumer<T2<String, Integer>> c, boolean failExp) {
        T2<String, Integer> t = entry();

        c.accept(t);

        assertThat(srvAllPerms.cache(CACHE_NAME).get(t.getKey()), failExp ? nullValue() : is(t.getValue()));
    }
}
