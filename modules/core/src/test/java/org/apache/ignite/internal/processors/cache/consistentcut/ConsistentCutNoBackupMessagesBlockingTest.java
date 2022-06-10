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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.cache.consistentcut.ConsistentCutBlockingCases.messages;

/** */
@RunWith(Parameterized.class)
public class ConsistentCutNoBackupMessagesBlockingTest extends AbstractConsistentCutBlockingTest {
    /** */
    @Parameterized.Parameter
    public BlkCutType cutBlkType;

    /** */
    @Parameterized.Parameter(1)
    public BlkNodeType cutNodeBlkType;

    /** */
    @Parameterized.Parameters(name = "cutBlkAt={0}, nodeBlk={1}")
    public static List<Object[]> params() {
        List<Object[]> p = new ArrayList<>();

        Stream.of(BlkCutType.NONE, BlkCutType.PUBLISH, BlkCutType.WAL_START).forEach(c ->
            Stream.of(BlkNodeType.NEAR, BlkNodeType.PRIMARY).forEach(n ->
                p.add(new Object[] {c, n})
            )
        );

        return p;
    }

    /** */
    @Test
    public void testOnePhaseCommitCases() throws Exception {
        List<List<T2<Integer, Integer>>> cases = ConsistentCutBlockingCases.casesNoBackup(nodes());

        List<String> msgs = messages(false);

        for (String msg: msgs) {
            initMsgCase(msg, cutBlkType, cutNodeBlkType);

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
