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

package org.apache.ignite.internal.client.impl;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Simple unit test for GridClientComputeImpl which checks method parameters.
 * It tests only those methods that can produce assertion underneath upon incorrect arguments.
 */
public class ClientComputeImplSelfTest extends GridCommonAbstractTest {
    /** Mocked client compute. */
    private GridClientCompute compute = allocateInstance0(GridClientComputeImpl.class);

    /**
     * @throws Exception If failed.
     */
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
    public void testRefreshNodeByIpAsync() throws Exception {
        assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return compute.refreshNode((String)null, false, false);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: ip");
    }
}