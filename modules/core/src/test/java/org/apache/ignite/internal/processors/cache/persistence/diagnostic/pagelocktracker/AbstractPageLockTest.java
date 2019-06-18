/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.log.PageLockLogSnapshot;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.junit.Assert.assertEquals;

/** */
public abstract class AbstractPageLockTest {
    /** */
    protected void doRunnable(int deep, Runnable r) {
        for (int i = 0; i < deep; i++)
            r.run();
    }

    /** */
    protected void awaitRandom(int bound) {
        try {
            U.sleep(nextRandomWaitTimeout(bound));
        }
        catch (IgniteInterruptedCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /** */
    protected int nextRandomWaitTimeout(int bound) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        return rnd.nextInt(bound);
    }

    /** */
    protected void checkNextOp(PageLockLogSnapshot lockLog, long nextOpPageId, long nextOp, int nextOpStructureId) {
        assertEquals(nextOpStructureId, lockLog.nextOpStructureId);
        assertEquals(nextOp, lockLog.nextOp);
        assertEquals(nextOpPageId, lockLog.nextOpPageId);
    }
}
