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
import java.util.List;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class IncrementalSnapshotNoBackupMessagesBlockingTest extends AbstractIncrementalSnapshotMessagesBlockingTest {
    /** */
    @Parameterized.Parameter
    public BlkNodeType txNodeBlkType;

    /** */
    @Parameterized.Parameter(1)
    public BlkSnpType snpBlkType;

    /** */
    @Parameterized.Parameter(2)
    public BlkNodeType snpNodeBlkType;

    /** */
    @Parameterized.Parameters(name = "txNodeBlk={0}, snpBlkAt={1}, snpNodeBlk={2}")
    public static List<Object[]> params() {
        List<Object[]> p = new ArrayList<>();

        Stream.of(BlkNodeType.NEAR, BlkNodeType.PRIMARY).forEach(txN ->
            Stream.of(BlkNodeType.NEAR, BlkNodeType.PRIMARY).forEach(snpN -> {
                for (BlkSnpType c : BlkSnpType.values())
                    p.add(new Object[] {txN, c, snpN});
            })
        );

        return p;
    }

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        List<TransactionTestCase> cases = TransactionTestCase.buildTestCases(nodes(), false);

        List<Class<?>> msgs = messages(false);

        for (Class<?> msg: msgs) {
            initMsgCase(msg, txNodeBlkType, snpBlkType, snpNodeBlkType);

            runCases(cases);
        }

        checkWalsConsistency();
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
