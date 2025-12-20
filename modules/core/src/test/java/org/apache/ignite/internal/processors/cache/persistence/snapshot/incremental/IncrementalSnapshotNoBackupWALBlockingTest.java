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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.incremental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.transactions.TransactionState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.PREPARED;

/** */
@ParameterizedClass(name = "txStateBlk={0}, txNodeBlk={1}, snpBlkType={2}, snpBlkNode={3}")
@MethodSource("allTypesArgs")
public class IncrementalSnapshotNoBackupWALBlockingTest extends AbstractIncrementalSnapshotWalBlockingTest {
    /** */
    @Parameter
    private TransactionState txBlkState;

    /** */
    @Parameter(1)
    private BlkNodeType txBlkNode;

    /** */
    @Parameter(2)
    private BlkSnpType snpBlkType;

    /** */
    @Parameter(3)
    private BlkNodeType snpBlkNode;

    /** */
    private static Collection<Arguments> allTypesArgs() {
        List<Arguments> params = new ArrayList<>();

        Stream.of(PREPARED, COMMITTED).forEach((tx) ->
            Stream.of(BlkNodeType.NEAR, BlkNodeType.PRIMARY).forEach((nt) ->
                Stream.of(BlkNodeType.NEAR, BlkNodeType.PRIMARY).forEach(nc -> {
                    for (BlkSnpType c : BlkSnpType.values())
                        params.add(Arguments.of(tx, nt, c, nc));
                })
            )
        );

        return params;
    }

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        initWALCase(txBlkState, txBlkNode, snpBlkType, snpBlkNode);

        runCases(cases());

        checkWalsConsistency();
    }

    /** */
    protected List<TransactionTestCase> cases() {
        return TransactionTestCase.buildTestCases(nodes(), false);
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
