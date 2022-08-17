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
import org.apache.ignite.internal.util.typedef.T2;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class ConsistentCutSingleBackupMessagesBlockingTest extends AbstractConsistentCutBlockingTest {
    /** */
    @Parameterized.Parameter
    public BlkNodeType txNodeBlkType;

    /** */
    @Parameterized.Parameter(1)
    public BlkCutType cutBlkType;

    /** */
    @Parameterized.Parameter(2)
    public BlkNodeType cutNodeBlkType;

    /** */
    @Parameterized.Parameters(name = "txNodeBlk={0}, cutBlkAt={1}, cutNodeBlk={2}")
    public static List<Object[]> params() {
        List<Object[]> p = new ArrayList<>();

        for (BlkNodeType txN: BlkNodeType.values()) {
            for (BlkNodeType cutN: BlkNodeType.values()) {
                for (BlkCutType c : BlkCutType.blkTestValues())
                    p.add(new Object[] {txN, c, cutN});
            }
        }

        return p;
    }

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        List<List<T2<Integer, Integer>>> cases = ConsistentCutBlockingCases.casesWithBackup(nodes());

        List<Class<?>> msgs = ConsistentCutBlockingCases.messages(true);

        for (Class<?> msg: msgs) {
            initMsgCase(msg, txNodeBlkType, cutBlkType, cutNodeBlkType);

            runCases(cases, false);
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
