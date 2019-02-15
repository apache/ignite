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

package org.apache.ignite.internal.client.impl;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Simple unit test for GridClientComputeImpl which checks method parameters.
 * It tests only those methods that can produce assertion underneath upon incorrect arguments.
 */
@RunWith(JUnit4.class)
public class ClientComputeImplSelfTest extends GridCommonAbstractTest {
    /** Mocked client compute. */
    private GridClientCompute compute = allocateInstance0(GridClientComputeImpl.class);

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testProjection_byGridClientNode() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.projection((GridClientNode)null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: node");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecute() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.execute(null, null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testExecuteAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.executeAsync(null, null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityExecute() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.affinityExecute(null, "cache", "key", null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testAffinityExecuteAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.affinityExecute(null, "cache", "key", null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: taskName");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNode() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.node(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: id");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodesByIds() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.nodes((Collection<UUID>)null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: ids");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNodesByFilter() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.nodes((GridClientPredicate<GridClientNode>)null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: filter");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRefreshNodeById() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNode((UUID)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: id");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRefreshNodeByIdAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNodeAsync((UUID)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: id");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRefreshNodeByIp() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNode((String)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: ip");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRefreshNodeByIpAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNode((String)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: ip");
    }
}
