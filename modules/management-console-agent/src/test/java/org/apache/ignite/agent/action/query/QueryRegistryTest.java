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

package org.apache.ignite.agent.action.query;

import java.util.ArrayList;
import org.apache.ignite.IgniteException;
import org.apache.ignite.agent.AgentCommonAbstractTest;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Query registry test.
 */
public class QueryRegistryTest extends AgentCommonAbstractTest {
    /**
     * Should remove expired holders.
     */
    @Test
    public void shouldRemoveExpiredHolders() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        QueryHolderRegistry registry = new QueryHolderRegistry(ignite.context(), 100);

        String qryId = "qry";

        registry.createQueryHolder(qryId);

        String cursorId = registry.addCursor(qryId, new CursorHolder(new QueryCursorImpl<>(new ArrayList<>())));

        Thread.sleep(300);

        GridTestUtils.assertThrows(null, () -> {
            registry.findCursor(qryId, cursorId);

            return null;
        }, IgniteException.class, "Query results are expired.");
    }

    /**
     * Should not remove holder if they was fetched.
     */
    @Test
    public void shouldNotRemoveExpiredHoldersIfTheyWasFetched() throws Exception {
        IgniteEx ignite = (IgniteEx) startGrid();

        QueryHolderRegistry registry = new QueryHolderRegistry(ignite.context(), 200);

        String qryId = "qry";

        registry.createQueryHolder(qryId);

        String curId = registry.addCursor(qryId, new CursorHolder(new QueryCursorImpl<>(new ArrayList<>())));

        for (int i = 0; i < 5; i++) {
            Thread.sleep(100);

            registry.findCursor(qryId, curId);
        }

        Thread.sleep(600);

        GridTestUtils.assertThrows(null, () -> {
            registry.findCursor(qryId, curId);

            return null;
        }, IgniteException.class, "Query results are expired.");
    }
}
