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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** */
@ParameterizedClass(name = "txNodeBlk={0}, snpBlkAt={1}, nodeBlk={2}, blkMsgCls={3}")
@MethodSource("allTypesArgs")
public class IncrementalSnapshotTwoBackupMessagesBlockingTest extends AbstractIncrementalSnapshotMessagesBlockingTest {
    /** */
    @Parameter(0)
    public BlkNodeType txNodeBlkType;

    /** */
    @Parameter(1)
    public BlkSnpType snpBlkType;

    /** */
    @Parameter(2)
    public BlkNodeType snpNodeBlkType;

    /** */
    @Parameter(3)
    public Class<?> blkMsgCls;

    /** */
    private static Collection<Arguments> allTypesArgs() {
        List<Arguments> params = new ArrayList<>();

        List<Class<?>> msgs = messages(true);

        for (BlkNodeType txN: BlkNodeType.values()) {
            for (BlkNodeType snpN: BlkNodeType.values()) {
                for (BlkSnpType c : BlkSnpType.values()) {
                    for (Class<?> m: msgs)
                        params.add(Arguments.of(txN, c, snpN, m));
                }
            }
        }

        return params;
    }

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        List<TransactionTestCase> cases = TransactionTestCase.buildTestCases(nodes(), true);

        initMsgCase(blkMsgCls, txNodeBlkType, snpBlkType, snpNodeBlkType);

        runCases(cases);

        checkWalsConsistency();
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 2;
    }
}
