/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors;

import java.time.Instant;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTrackerDump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.ThreadPageLockState;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Unit tests for {@link ToStringDumpHelper}.
 */
public class ToStringDumpHelperTest extends GridCommonAbstractTest {
    /** */
    private static final long TIME = 1596173397167L;

    /** */
    @Test
    public void toStringSharedPageLockTrackerTest() throws Exception {
        SharedPageLockTracker pageLockTracker = new SharedPageLockTracker();

        PageLockListener tracker = pageLockTracker.registerStructure("dummy");

        tracker.onReadLock(1, 2, 3, 4);

        tracker.onReadUnlock(1, 2, 3, 4);

        Thread asyncLockUnlock = new Thread(() -> tracker.onReadLock(4, 32, 1, 64), "async-lock-unlock");
        asyncLockUnlock.start();
        asyncLockUnlock.join();

        long threadIdInLog = asyncLockUnlock.getId();

        SharedPageLockTrackerDump pageLockDump = pageLockTracker.dump();

        // Hack to have same timestamp in test.
        for (ThreadPageLockState state : pageLockDump.threadPageLockStates)
            GridTestUtils.setFieldValue(state.pageLockDump, PageLockDump.class, "time", TIME);

        assertNotNull(pageLockDump);

        String dumpStr = ToStringDumpHelper.toStringDump(pageLockDump);

        String expectedLog = "Page locks dump:" + U.nl() +
            U.nl() +
            "Thread=[name=async-lock-unlock, id=" + threadIdInLog + "], state=TERMINATED" + U.nl() +
            "Locked pages = [32[0000000000000020](r=1|w=0)]" + U.nl() +
            "Locked pages log: name=async-lock-unlock time=(1596173397167, " +
            ToStringDumpHelper.DATE_FMT.format(Instant.ofEpochMilli(TIME)) + ")" + U.nl() +
            "L=1 -> Read lock pageId=32, structureId=dummy [pageIdHex=0000000000000020, partId=0, pageIdx=32, flags=00000000]" +
            U.nl() +
            U.nl() +
            U.nl() +
            U.nl();

        assertEquals(expectedLog, dumpStr);
    }
}
