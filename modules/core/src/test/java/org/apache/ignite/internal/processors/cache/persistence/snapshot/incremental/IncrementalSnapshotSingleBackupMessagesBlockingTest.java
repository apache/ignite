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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** */
public class IncrementalSnapshotSingleBackupMessagesBlockingTest extends AbstractIncrementalSnapshotMessagesBlockingTest {
    /** */
    private static Collection<Arguments> allTypesArgs() {
        List<Arguments> params = new ArrayList<>();

        for (BlkNodeType txN: BlkNodeType.values()) {
            for (BlkNodeType snpN: BlkNodeType.values()) {
                for (BlkSnpType c : BlkSnpType.values())
                    params.add(Arguments.of(txN, c, snpN));
            }
        }

        return params;
    }

    /** */
    @ParameterizedTest(name = "txNodeBlk={0}, snpBlkAt={1}, snpNodeBlk={2}")
    @MethodSource("allTypesArgs")
    public void testMultipleCases(BlkNodeType txNodeBlkType, BlkSnpType snpBlkType, BlkNodeType snpNodeBlkType) throws Exception {
        List<TransactionTestCase> cases = TransactionTestCase.buildTestCases(nodes(), true);

        List<Class<?>> msgs = messages(true);

        for (Class<?> msg: msgs) {
            initMsgCase(msg, txNodeBlkType, snpBlkType, snpNodeBlkType);

            runCases(cases);
        }

        checkWalsConsistency();
    }

    /** {@inheritDoc} */
    @Override protected int nodes() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected int backups() {
        return 1;
    }
}
