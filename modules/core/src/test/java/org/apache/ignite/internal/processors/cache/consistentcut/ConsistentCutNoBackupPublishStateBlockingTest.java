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

import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.NEAR;
import static org.apache.ignite.internal.processors.cache.consistentcut.AbstractConsistentCutBlockingTest.NodeType.PRIMARY;

/** */
@RunWith(Parameterized.class)
public class ConsistentCutNoBackupPublishStateBlockingTest extends AbstractConsistentCutPublishStateBlockingTest {
    /** */
    @Parameterized.Parameter
    public AbstractConsistentCutWALBlockingTest.NodeType nodeType;

    /** */
    @Parameterized.Parameters(name = "blkNode={0}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        Stream.of(NEAR, PRIMARY).forEach((nt) ->
            params.add(new Object[] {nt})
        );

        return params;
    }

    /** */
    @Test
    public void testOnePhaseCommitCases() throws Exception {
        List<List<T2<Integer, Integer>>> cases = ConsistentCutBlockingCases.casesNoBackup(2);

        List<String> msgs = messagesNoBackups(true);

        for (String msg: msgs) {
            blkMsgCls = msg;
            blkNodeType = nodeType;

            runCases(cases);
        }

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
