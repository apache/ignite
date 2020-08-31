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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Arrays;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests cache query configuration.
 */
public class IgniteCacheDistributedPartitionQueryConfigurationSelfTest extends GridCommonAbstractTest {
    /** Tests partition validation. */
    @Test
    public void testPartitions() {
        final SqlFieldsQuery qry = new SqlFieldsQuery("select 1");

        // Empty set is not allowed.
        failIfNotThrown(new Runnable() {
            @Override public void run() {
                qry.setPartitions();
            }
        });

        // Duplicates are not allowed.
        failIfNotThrown(new Runnable() {
            @Override public void run() {
                qry.setPartitions(0, 1, 2, 1);
            }
        });

        // Values out of range are not allowed.
        failIfNotThrown(new Runnable() {
            @Override public void run() {
                qry.setPartitions(-1, 0, 1);
            }
        });

        // Duplicates with unordered input are not allowed.
        failIfNotThrown(new Runnable() {
            @Override public void run() {
                qry.setPartitions(3, 2, 2);
            }
        });

        // Values out of range are not allowed.
        failIfNotThrown(new Runnable() {
            @Override public void run() {
                qry.setPartitions(-1, 0, 1);
            }
        });

        // Expecting ordered set.
        int[] tmp = new int[] {6, 2, 3};
        qry.setPartitions(tmp);

        assertTrue(Arrays.equals(new int[]{2, 3, 6}, tmp));

        // If already ordered expecting same instance.
        qry.setPartitions((tmp = new int[] {0, 1, 2}));

        assertTrue(tmp == qry.getPartitions());
    }

    /**
     * @param r Runnable.
     */
    private void failIfNotThrown(Runnable r) {
        try {
            r.run();

            fail();
        }
        catch (Exception ignored) {
            // No-op.
        }
    }
}
