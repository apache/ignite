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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.PartitionUpdateCounter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.Test;

/**
 * Tests if updates using new counter implementation is applied in expected order.
 */
public class TxPartitionCounterStateUpdatesOrderTest extends TxPartitionCounterStateAbstractTest {
    /** */
    public static final int PARTITION_ID = 0;

    /**
     * Should observe same order of updates on all owners.
     * @throws Exception
     */
    @Test
    public void testSingleThreadedUpdateOrder() throws Exception {
        backups = 3;

        Ignite crd = startGridsMultiThreaded(3);

        IgniteEx client = startGrid("client");

        IgniteCache<Object, Object> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

        List<Integer> keys = partitionKeys(cache, PARTITION_ID, 100, 0);

        cache.put(keys.get(0), new TestVal(keys.get(0)));
        cache.put(keys.get(1), new TestVal(keys.get(1)));
        cache.put(keys.get(2), new TestVal(keys.get(2)));

        assertCountersSame(PARTITION_ID, false);

        cache.remove(keys.get(2));
        cache.remove(keys.get(1));
        cache.remove(keys.get(0));

        assertCountersSame(PARTITION_ID, false);

        WALIterator iter = walIterator((IgniteEx)crd);

        while(iter.hasNext()) {
            IgniteBiTuple<WALPointer, WALRecord> tup = iter.next();

            if (tup.get2() instanceof DataRecord)
                log.info("next " + tup.get2());
        }

        System.out.println();
    }

    private WALIterator walIterator(IgniteEx ignite) throws IgniteCheckedException {
        IgniteWriteAheadLogManager walMgr = ignite.context().cache().context().wal();

        return walMgr.replay(null);
    }

    /** */
    private static class TestVal {
        /** */
        int id;

        /**
         * @param id Id.
         */
        public TestVal(int id) {
            this.id = id;
        }
    }
}
