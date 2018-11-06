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

package org.apache.ignite.internal.processor.security;

import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiPredicate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for IgniteMessaging.
 * //todo DRAFT !!!
 */
public class IgniteMessagingTest extends AbstractContextResolverSecurityProcessorTest {
    /** Barrier. */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(2);

    /** */
    public void testMessaging() throws Exception {
        //todo Тесты написаны так, что инициатор так же является источником собтия.
        //todo Нужно написать тесты когда иточником события является другой узел
        //todo имеющий разрешение на выполнение операции и нет, соответственно.

        successMessaging(clnt, clntNoPutPerm);
        successMessaging(clnt, srvNoPutPerm);
        successMessaging(srv, clntNoPutPerm);
        successMessaging(srv, srvNoPutPerm);
        //successMessaging(srv, srv);
        //successMessaging(clnt, clnt);

        failMessaging(clntNoPutPerm, srv);
        failMessaging(clntNoPutPerm, clnt);
        failMessaging(srvNoPutPerm, srv);
        failMessaging(srvNoPutPerm, clnt);
        //failMessaging(srvNoPutPerm, srvNoPutPerm);
        //failMessaging(clntNoPutPerm, clntNoPutPerm);
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     * @throws Exception If failed.
     */
    private void successMessaging(IgniteEx initiator, IgniteEx remote) throws Exception {
        BARRIER.reset();

        int val = values.getAndIncrement();

        IgniteMessaging messaging = initiator.message(
            initiator.cluster().forNode(remote.localNode())
        );

        UUID lsnrId = messaging.remoteListen("HOT_TOPIC",
            new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    try {
                        Ignition.localIgnite().cache(CACHE_NAME).put("key", val);

                        return true;
                    }
                    finally {
                        try {
                            BARRIER.await(5, TimeUnit.SECONDS);
                        }
                        catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                            fail(e.getMessage());
                        }
                    }
                }
            }
        );
        try {
            messaging.send("HOT_TOPIC", "Fire!");

            BARRIER.await(5, TimeUnit.SECONDS);
        }
        finally {
            messaging.stopRemoteListen(lsnrId);
        }

        assertThat(remote.cache(CACHE_NAME).get("key"), is(val));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remote node.
     * @throws Exception If failed.
     */
    private void failMessaging(IgniteEx initiator, IgniteEx remote) throws Exception {
        BARRIER.reset();

        IgniteMessaging messaging = initiator.message(
            initiator.cluster().forNode(remote.localNode())
        );

        UUID lsnrId = messaging.remoteListen("HOT_TOPIC",
            new IgniteBiPredicate<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    try {
                        Ignition.localIgnite().cache(CACHE_NAME).put("fail_key", -1);

                        return true;
                    }
                    finally {
                        try {
                            BARRIER.await(5, TimeUnit.SECONDS);
                        }
                        catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                            fail(e.getMessage());
                        }
                    }
                }
            }
        );
        try {
            messaging.send("HOT_TOPIC", "Fire!");

            BARRIER.await(5, TimeUnit.SECONDS);
        }
        finally {
            messaging.stopRemoteListen(lsnrId);
        }

        assertThat(remote.cache(CACHE_NAME).get("fail_key"), nullValue());
    }
}
