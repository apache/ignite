/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.Test;

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
    @Test
    public void testDifferentListeners() {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

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
    @Test
    public void testMultipleExecutionsWithoutListeners() {
        checkLoop(1001);
    }

    /**
     * This is the workaround- as long as we keep a message listener in
     * the stack, our FIFO bug isn't exposed.  Comment above out to see.
     */
    @Test
    public void testOneListener() {
        Ignite ignite = G.ignite(getTestIgniteInstanceName());

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
    @Test
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
                G.ignite(getTestIgniteInstanceName()).compute().execute(t.getClass(), null);
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
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            // No-op.
        }
    }
}
