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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class CacheMvccProcessorTest extends CacheMvccAbstractTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTreeWithPersistence() throws Exception {
        persistence = true;

        checkTreeOperations();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTreeWithoutPersistence() throws Exception {
        persistence = false;

        checkTreeOperations();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTreeOperations() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().active(true);

        grid.createCache(new CacheConfiguration<>("test").setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT));

        MvccProcessorImpl mvccProcessor = mvccProcessor(grid);

        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA)));

        grid.context().cache().context().database().checkpointReadLock();
        try {
            mvccProcessor.updateState(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA), TxState.PREPARED);
            mvccProcessor.updateState(new MvccVersionImpl(1, 2, MvccUtils.MVCC_OP_COUNTER_NA), TxState.PREPARED);
            mvccProcessor.updateState(new MvccVersionImpl(1, 3, MvccUtils.MVCC_OP_COUNTER_NA), TxState.COMMITTED);
            mvccProcessor.updateState(new MvccVersionImpl(1, 4, MvccUtils.MVCC_OP_COUNTER_NA), TxState.ABORTED);
            mvccProcessor.updateState(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA), TxState.ABORTED);
            mvccProcessor.updateState(new MvccVersionImpl(1, 6, MvccUtils.MVCC_OP_COUNTER_NA), TxState.PREPARED);
        }
        finally {
            grid.context().cache().context().database().checkpointReadUnlock();
        }

        if (persistence) {
            stopGrid(0, false);
            grid = startGrid(0);

            grid.cluster().active(true);

            mvccProcessor = mvccProcessor(grid);
        }

        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 2, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.COMMITTED, mvccProcessor.state(new MvccVersionImpl(1, 3, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.ABORTED, mvccProcessor.state(new MvccVersionImpl(1, 4, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.ABORTED, mvccProcessor.state(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 6, MvccUtils.MVCC_OP_COUNTER_NA)));

        mvccProcessor.removeUntil(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA));

        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 1, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 2, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 3, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 4, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 5, MvccUtils.MVCC_OP_COUNTER_NA)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 6, MvccUtils.MVCC_OP_COUNTER_NA)));
    }
}
