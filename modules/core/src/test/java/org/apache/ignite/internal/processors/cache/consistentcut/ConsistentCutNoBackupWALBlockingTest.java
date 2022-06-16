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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.transactions.TransactionState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.BlkNodeType.PRIMARY;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.PREPARED;

/** */
@RunWith(Parameterized.class)
public class ConsistentCutNoBackupWALBlockingTest extends AbstractConsistentCutBlockingTest {
    /** */
    @Parameterized.Parameter
    public TransactionState txBlkState;

    /** */
    @Parameterized.Parameter(1)
    public BlkNodeType txBlkNode;

    /** */
    @Parameterized.Parameter(2)
    public BlkCutType cutBlkType;

    /** */
    @Parameterized.Parameter(3)
    public BlkNodeType cutBlkNode;

    /** */
    @Parameterized.Parameters(name = "txStateBlk={0}, txNodeBlk={1}, cutBlkType={2}, cutBlkNode={3}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        Stream.of(PREPARED, COMMITTED).forEach((tx) ->
            Stream.of(NEAR, PRIMARY).forEach((nt) ->
                Stream.of(NEAR, PRIMARY).forEach(nc ->
                    Stream.of(BlkCutType.NONE, BlkCutType.VERSION_UPDATE, BlkCutType.PUBLISH).forEach(c ->
                        params.add(new Object[] {tx, nt, c, nc})
                    )
                )
            )
        );

        return params;
    }

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        initWALCase(txBlkState, txBlkNode, cutBlkType, cutBlkNode);

        runCases(cases());

        checkWalsConsistency();
    }

    /** */
    protected List<List<T2<Integer, Integer>>> cases() {
        return ConsistentCutBlockingCases.casesNoBackup(nodes());
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 0;
    }
}
