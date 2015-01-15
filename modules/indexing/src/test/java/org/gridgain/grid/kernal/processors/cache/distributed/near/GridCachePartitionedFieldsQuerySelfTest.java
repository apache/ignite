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

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests for fields queries.
 */
public class GridCachePartitionedFieldsQuerySelfTest extends GridCacheAbstractFieldsQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Distribution.
     */
    protected GridCacheDistributionMode distributionMode() {
        return NEAR_PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cache(@Nullable String name, @Nullable String spiName) {
        GridCacheConfiguration cc = super.cache(name, spiName);

        cc.setDistributionMode(distributionMode());

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncludeBackups() throws Exception {
        GridCacheQuery<List<?>> qry = grid(0).cache(null).queries().createSqlFieldsQuery(
            "select _KEY, name, age from Person");

        qry.includeBackups(true);

        GridCacheQueryFuture<List<?>> fut = qry.execute();

        List<List<?>> res = new ArrayList<>(fut.get());

        assertNotNull("Result", res);
        assertEquals("Result", res.size(), 6);

        Collections.sort(res, new Comparator<List<?>>() {
            @Override public int compare(List<?> row1, List<?> row2) {
                return ((Integer)row1.get(2)).compareTo((Integer)row2.get(2));
            }
        });

        int cnt = 0;

        for (List<?> row : res) {
            assertEquals("Row size", 3, row.size());

            if (cnt == 0 || cnt == 1) {
                assertEquals("Key", new GridCacheAffinityKey<>("p1", "o1"), row.get(0));
                assertEquals("Name", "John White", row.get(1));
                assertEquals("Age", 25, row.get(2));
            }
            else if (cnt == 2 || cnt == 3) {
                assertEquals("Key", new GridCacheAffinityKey<>("p2", "o1"), row.get(0));
                assertEquals("Name", "Joe Black", row.get(1));
                assertEquals("Age", 35, row.get(2));
            }
            else if (cnt == 4 || cnt == 5) {
                assertEquals("Key", new GridCacheAffinityKey<>("p3", "o2"), row.get(0));
                assertEquals("Name", "Mike Green", row.get(1));
                assertEquals("Age", 40, row.get(2));
            }

            cnt++;
        }

        assertEquals("Result count", 6, cnt);
    }
}
