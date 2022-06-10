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

/** */
@RunWith(Parameterized.class)
public class ConsistentCutTwoBackupMessagesBlockingTest extends AbstractConsistentCutBlockingTest {
    /** */
    @Parameterized.Parameter
    public BlkCutType cutBlkType;

    /** */
    @Parameterized.Parameter(1)
    public BlkNodeType cutNodeBlkType;

    /** */
    @Parameterized.Parameter(2)
    public String blkMsgCls;

    /** */
    @Parameterized.Parameters(name = "cutBlkAt={0}, nodeBlk={1}, blkMsgCls={2}")
    public static List<Object[]> params() {
        List<Object[]> p = new ArrayList<>();

        List<String> msgs = ConsistentCutBlockingCases.messages(true);

        Stream.of(BlkCutType.NONE, BlkCutType.PUBLISH, BlkCutType.WAL_START).forEach(c ->
            Stream.of(BlkNodeType.NEAR, BlkNodeType.PRIMARY, BlkNodeType.BACKUP).forEach(n -> {
                for (String m: msgs)
                    p.add(new Object[] {c, n, m});
            })
        );

        return p;
    }

    /** */
    @Test
    public void testMultipleCases() throws Exception {
        List<List<T2<Integer, Integer>>> cases = ConsistentCutBlockingCases.casesWithBackup(nodes());

        initMsgCase(blkMsgCls, cutBlkType, cutNodeBlkType);

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
