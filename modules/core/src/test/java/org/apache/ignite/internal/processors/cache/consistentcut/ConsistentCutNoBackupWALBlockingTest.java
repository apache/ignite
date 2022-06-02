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

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.PRIMARY;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.PREPARED;

/** */
@RunWith(Parameterized.class)
public class ConsistentCutNoBackupWALBlockingTest extends AbstractConsistentCutWALBlockingTest {
    /** */
    @Parameterized.Parameter
    public TransactionState txState;

    /** */
    @Parameterized.Parameter(1)
    public NodeType nodeType;

    /** */
    @Parameterized.Parameters(name = "txBlkState={0}, blkNode={1}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        Stream.of(PREPARED, COMMITTED).forEach((tx) ->
            Stream.of(NEAR, PRIMARY).forEach((nt) ->
                params.add(new Object[] {tx, nt})
            )
        );

        return params;
    }

    /** */
    @Test
    public void testOnePhaseCommitCases() throws Exception {
        List<List<T2<Integer, Integer>>> cases = ConsistentCutBlockingCases.casesNoBackup(nodes());

        blkNodeType = nodeType;
        blkTxState = txState;

        runCases(cases);

        checkWals(txOrigNode, caseNum, caseNum);
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
