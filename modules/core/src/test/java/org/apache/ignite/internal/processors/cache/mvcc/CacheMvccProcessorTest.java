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

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.mvcc.txlog.TxState;

/**
 *
 */
public class CacheMvccProcessorTest extends CacheMvccAbstractTest {

    /**
     * @throws Exception If failed.
     */
    public void testTreeWithPersistence() throws Exception {
        persistence = true;

        checkTreeOperations();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTreeWithoutPersistence() throws Exception {
        persistence = true;

        checkTreeOperations();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkTreeOperations() throws Exception {
        IgniteEx grid = startGrid(0);

        grid.cluster().active(true);

        MvccProcessor mvccProcessor = grid.context().coordinators();

        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 1)));

        mvccProcessor.updateState(new MvccVersionImpl(1, 1), TxState.PREPARED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 2), TxState.PREPARED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 3), TxState.COMMITTED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 4), TxState.ABORTED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 5), TxState.ABORTED);
        mvccProcessor.updateState(new MvccVersionImpl(1, 6), TxState.PREPARED);

        if (persistence) {
            stopGrid(0, false);
            grid = startGrid(0);

            grid.cluster().active(true);

            mvccProcessor = grid.context().coordinators();
        }

        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 1)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 2)));
        assertEquals(TxState.COMMITTED, mvccProcessor.state(new MvccVersionImpl(1, 3)));
        assertEquals(TxState.ABORTED, mvccProcessor.state(new MvccVersionImpl(1, 4)));
        assertEquals(TxState.ABORTED, mvccProcessor.state(new MvccVersionImpl(1, 5)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 6)));

        mvccProcessor.removeUntil(new MvccVersionImpl(1, 5));

        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 1)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 2)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 3)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 4)));
        assertEquals(TxState.NA, mvccProcessor.state(new MvccVersionImpl(1, 5)));
        assertEquals(TxState.PREPARED, mvccProcessor.state(new MvccVersionImpl(1, 6)));
    }
}
