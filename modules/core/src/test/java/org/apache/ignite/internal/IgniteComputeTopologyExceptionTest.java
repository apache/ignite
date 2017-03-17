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

package org.apache.ignite.internal;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.GridClosureCallMode.BALANCE;

/**
 *
 */
public class IgniteComputeTopologyExceptionTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectException() throws Exception {
        Ignite ignite = ignite(0);

        IgniteCompute comp = ignite.compute(ignite.cluster().forRemotes()).withNoFailover();

        stopGrid(1);

        try {
            comp.call(new IgniteCallable<Object>() {
                @Override public Object call() throws Exception {
                    fail("Should not be called.");

                    return null;
                }
            });

            fail();
        }
        catch (ClusterTopologyException e) {
            log.info("Expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCorrectCheckedException() throws Exception {
        IgniteKernal ignite0 = (IgniteKernal)ignite(0);

        Collection<ClusterNode> nodes = F.asList(ignite(1).cluster().localNode());

        stopGrid(1);

        IgniteInternalFuture<?> fut = ignite0.context().closure().callAsyncNoFailover(BALANCE,
            new IgniteCallable<Object>() {
                @Override public Object call() throws Exception {
                    fail("Should not be called.");

                    return null;
                }
            },
            nodes,
            false,
            0);

        try {
            fut.get();

            fail();
        }
        catch (ClusterTopologyCheckedException e) {
            log.info("Expected exception: " + e);
        }
    }
}
