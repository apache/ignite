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

package org.apache.ignite.common;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * {@inheritDoc} With topology events in parallel
 */
public class ClientSideCacheCreationDestructionWileTopologyChangeTest extends ClientSizeCacheCreationDestructionTest {
    /** **/
    private static final int MAX_NODES_CNT = 10;

    /** **/
    IgniteInternalFuture topChangeProcFut;

    /** **/
    AtomicBoolean procTopChanges = new AtomicBoolean(true);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        topChangeProcFut = asyncTopologyChanges();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        procTopChanges.set(false);

        topChangeProcFut.get();

        super.afterTest();
    }

    /**
     * @return {@code IgniteInternalFuture} to wait for topology process to stop in {@code afterTest()}.
     */
    private IgniteInternalFuture asyncTopologyChanges() {
        return GridTestUtils.runAsync(() -> {
            while (procTopChanges.get()) {
                try {
                    if (srv.cluster().nodes().size() < MAX_NODES_CNT)
                        startGrid(UUID.randomUUID().toString());
                }
                catch (Exception e) {
                    fail("Unable to add or remove node: " + e);
                }
            }
        });
    }
}
