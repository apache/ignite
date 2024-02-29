/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite;

import java.net.DnsBlocker;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks {@link ClusterState} change.
 */
public class DnsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 1_000_000;
    }

    /** */
    @Test
    public void test() throws Exception {
        DnsBlocker.INSTANCE.unblock();

        Ignite ignite = startGrids(3);

        ignite.cluster().state(ClusterState.ACTIVE);

        DnsBlocker.INSTANCE.block();

        startGrid(3);
        stopGrid(3);
        ignite.getOrCreateCache("TEST");
        for (int i = 0; i < 100; i++)
            ignite.cache("TEST").put(i, i);
    }
}
