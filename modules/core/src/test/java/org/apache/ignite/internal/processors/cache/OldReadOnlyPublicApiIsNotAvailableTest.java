/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCluster;
import org.apache.ignite.internal.client.GridClientClusterState;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 *
 */
public class OldReadOnlyPublicApiIsNotAvailableTest extends GridCommonAbstractTest {
    /**
     * Test must be deleted after https://ggsystems.atlassian.net/browse/GG-25084
     */
    @Test
    public void testOldReadOnlyPublicApiIsNotAvailable() throws Exception {
        // TODO: Remove me after https://ggsystems.atlassian.net/browse/GG-25084

        assertThrows(log, () -> IgniteCluster.class.getMethod("readOnly", boolean.class), NoSuchMethodException.class, null);
        assertThrows(log, () -> IgniteCluster.class.getMethod("readOnly"), NoSuchMethodException.class, null);

        assertThrows(log, () -> GridClientClusterState.class.getMethod("readOnly", boolean.class), NoSuchMethodException.class, null);
        assertThrows(log, () -> GridClientClusterState.class.getMethod("readOnly"), NoSuchMethodException.class, null);
    }
}
