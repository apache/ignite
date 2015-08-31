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

package org.apache.ignite.internal.managers.communication;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid communication manager self test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridCommunicationManagerListenersSelfTest extends GridCommonAbstractTest {
    /** */
    public GridCommunicationManagerListenersSelfTest() {
        super(true);
    }

    /**
     * Works fine.
     */
    @SuppressWarnings({"deprecation"})
    public void testDifferentListeners() {
        Ignite ignite = G.ignite(getTestGridName());

        for (int i = 0; i < 2000; i++) {
            P2<UUID, Object> l = new P2<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    return false;
                }
            };

            ignite.message().localListen(null, l);
        }

        info(getName() + ": worked without exceptions.");
    }


    /**
     * Fails on the 1001st time.
     */
    public void testMultipleExecutionsWithoutListeners() {
        checkLoop(1001);
    }

    /**
     * This is the workaround- as long as we keep a message listener in
     * the stack, our FIFO bug isn't exposed.  Comment above out to see.
     */
    @SuppressWarnings({"deprecation"})
    public void testOneListener() {
        Ignite ignite = G.ignite(getTestGridName());

        final AtomicBoolean stop = new AtomicBoolean();

        P2<UUID, Object> l = new P2<UUID, Object>() {
            @Override public boolean apply(UUID uuid, Object o) {
                return stop.get();
            }
        };

        try {
            ignite.message().localListen(null, l);

            checkLoop(2000);
        } finally {
            stop.set(true);
        }
    }

    /**
     * Now, our test will fail on the first message added after our safety
     * message listener has been removed.
     */
    public void testSingleExecutionWithoutListeners() {
        checkLoop(1);
    }

    /**
     * @param cnt Iteration count.
     */
    private void checkLoop(int cnt) {
        for (int i = 1; i <= cnt; i++) {
            MessageListeningTask t = new MessageListeningTask();

            try {
                G.ignite(getTestGridName()).compute().execute(t.getClass(), null);
            }
            catch (IgniteException e) {
                assert false : "Failed to execute task [iteration=" + i + ", err=" + e.getMessage() + ']';
            }

            if (i % 100 == 0)
                info(getName() + ": through " + i);
        }
    }

    /**
     *
     */
    private static class MessageListeningTask extends ComputeTaskSplitAdapter<Object, Object>
        implements GridMessageListener {
        /** */
        @IgniteInstanceResource
        private transient Ignite ignite;

        /** */
        private AtomicBoolean stop = new AtomicBoolean();

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            ignite.message().localListen(null, new P2<UUID, Object>() {
                @Override public boolean apply(UUID uuid, Object o) {
                    return stop.get();
                }
            });

            return Arrays.asList(new ComputeJobAdapter() {
                @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            stop.set(true);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void onMessage(UUID nodeId, Object msg) {
            // No-op.
        }
    }
}