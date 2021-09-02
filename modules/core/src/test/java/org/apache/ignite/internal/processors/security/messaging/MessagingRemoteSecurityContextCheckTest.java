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

package org.apache.ignite.internal.processors.security.messaging;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Testing operation security context when the remote listener of IgniteMessaging is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to 'run' nodes that register the remote listener on check nodes. That listener
 * is executed on 'check' nodes and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification
 * that operation security context is the initiator context.
 */
public class MessagingRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Server node to change cache state. */
    private static final String SRV = "srv";

    /** Wait condition timeout. */
    private static final int WAIT_CONDITION_TIMEOUT = 10_000;

    /** Index to generate a unique topic and the synchronized set value. */
    private static final AtomicInteger TOPIC_INDEX = new AtomicInteger();

    /** */
    private static final Set<Object> SYNCHRONIZED_SET = Collections.synchronizedSet(new HashSet<>());

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV);

        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        startGridAllowAll(CLNT_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        startClientAllowAll(CLNT_ENDPOINT);

        awaitPartitionMapExchange();
    }

    /** */
    @Test
    public void testRemoteListen() {
        BiFunction<IgniteMessaging, String, UUID> f = new BiFunction<IgniteMessaging, String, UUID>() {
            @Override public UUID apply(IgniteMessaging m, String t) {
                return m.remoteListen(t, listener());
            }
        };

        execute(f);
    }

    /** */
    @Test
    public void testRemoteListenAsync() {
        BiFunction<IgniteMessaging, String, UUID> f = new BiFunction<IgniteMessaging, String, UUID>() {
            @Override public UUID apply(IgniteMessaging m, String t) {
                return m.remoteListenAsync(t, listener()).get();
            }
        };

        execute(f);
    }

    /** */
    private void execute(BiFunction<IgniteMessaging, String, UUID> func) {
        runAndCheck(() -> {
            Ignite loc = Ignition.localIgnite();

            IgniteMessaging messaging = loc.message(loc.cluster().forNodeIds(nodesToCheckIds()));

            Integer idx = TOPIC_INDEX.incrementAndGet();

            String topic = "test_topic_" + idx;

            UUID id = func.apply(messaging, topic);

            try {
                grid(SRV).message().send(topic, idx);

                wait(idx);
            }
            finally {
                messaging.stopRemoteListen(id);
            }
        });
    }

    /** */
    private IgniteBiPredicate<UUID, ?> listener() {
        return (uuid, o) -> {
            VERIFIER.register(OPERATION_CHECK);

            compute(Ignition.localIgnite(), endpointIds()).broadcast(() -> VERIFIER.register(OPERATION_ENDPOINT));

            SYNCHRONIZED_SET.add(o);

            return true;
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
