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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Checks that a remote listener for IgniteMessaging is executed inside the sandbox.
 */
public class MessagingSandboxTest extends AbstractSandboxTest {
    /** Wait condition timeout. */
    private static final int WAIT_CONDITION_TIMEOUT = 10_000;

    /** Index to generate a unique topic and the synchronized set value. */
    private static final AtomicInteger TOPIC_INDEX = new AtomicInteger();

    /** */
    private static final Set<Object> SYNCHRONIZED_SET = Collections.synchronizedSet(new HashSet<>());

    /** */
    @Test
    public void testRemoteListen() {
        testMessaging((m, t) -> m.remoteListen(t, listener()));
    }

    /** */
    @Test
    public void testRemoteListenAsync() {
        testMessaging((m, t) -> m.remoteListenAsync(t, listener()).get());
    }

    /** */
    private void testMessaging(BiFunction<IgniteMessaging, String, UUID> func) {
        execute(grid(CLNT_ALLOWED_WRITE_PROP), func, this::runOperation);
        execute(grid(CLNT_FORBIDDEN_WRITE_PROP), func, r -> runForbiddenOperation(r, AccessControlException.class));
    }

    /** */
    private void execute(Ignite node, BiFunction<IgniteMessaging, String, UUID> func,
        Consumer<GridTestUtils.RunnableX> op) {
        final Integer idx = TOPIC_INDEX.incrementAndGet();

        final String topic = "test_topic_" + idx;

        IgniteMessaging messaging = node.message(node.cluster().forNodeId(grid(SRV).localNode().id()));

        UUID listenerId = func.apply(messaging, topic);

        try {
            op.accept(() -> {
                grid(SRV).message().send(topic, idx);

                wait(idx);
            });
        }
        finally {
            messaging.stopRemoteListen(listenerId);
        }
    }

    /** */
    private IgniteBiPredicate<UUID, ?> listener() {
        return (uuid, o) -> {
            controlAction();

            SYNCHRONIZED_SET.add(o);

            return false;
        };
    }

    /** */
    private void wait(Integer idx) {
        try {
            GridTestUtils.waitForCondition(() -> SYNCHRONIZED_SET.contains(idx), WAIT_CONDITION_TIMEOUT);
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new RuntimeException(e);
        }
    }
}
