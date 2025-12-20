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

import org.junitpioneer.jupiter.cartesian.CartesianTest;
import java.util.List;

import static org.junitpioneer.jupiter.cartesian.CartesianTest.Enum.Mode.EXCLUDE;

/** */
public class IncrementalSnapshotNoBackupMessagesBlockingTest extends AbstractIncrementalSnapshotMessagesBlockingTest {
    /** */
    @CartesianTest(name = "txNodeBlk={0}, snpBlkAt={1}, snpNodeBlk={2}")
    public void testMultipleCases(
            @CartesianTest.Enum(value = BlkNodeType.class, mode = EXCLUDE, names = "BACKUP") BlkNodeType txNodeBlkType,
            @CartesianTest.Enum(value = BlkNodeType.class, mode = EXCLUDE, names = "BACKUP") BlkNodeType snpNodeBlkType,
            @CartesianTest.Enum(value = BlkSnpType.class) BlkSnpType snpBlkType
    ) throws Exception {
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
