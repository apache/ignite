/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests critical failure handling on checkpoint read lock acquisition errors.
 */
public class CheckpointReadLockFailureTest extends GridCommonAbstractTest {
    /** */
    private static final AbstractFailureHandler FAILURE_HND = new AbstractFailureHandler() {
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            if (failureCtx.type() != FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT)
                return true;

            if (hndLatch != null)
                hndLatch.countDown();

            return false;
        }
    };

    /** */
    private static volatile CountDownLatch hndLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(FAILURE_HND)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
                .setCheckpointFrequency(Integer.MAX_VALUE)
                .setCheckpointReadLockTimeout(1));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Set<FailureType> ignoredFailureTypes = new HashSet<>(FAILURE_HND.getIgnoredFailureTypes());
        ignoredFailureTypes.remove(FailureType.SYSTEM_CRITICAL_OPERATION_TIMEOUT);

        FAILURE_HND.setIgnoredFailureTypes(ignoredFailureTypes);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFailureTypeOnTimeout() throws Exception {
        hndLatch = new CountDownLatch(1);

        IgniteEx ig = startGrid(0);

        ig.cluster().active(true);

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)ig.context().cache().context().database();

        IgniteInternalFuture acquireWriteLock = GridTestUtils.runAsync(() -> {
            db.checkpointLock.writeLock().lock();

            try {
                doSleep(Long.MAX_VALUE);
            }
            finally {
                db.checkpointLock.writeLock().unlock();
            }
        });

        GridTestUtils.waitForCondition(() -> db.checkpointLock.writeLock().isHeldByCurrentThread(), 5000);

        IgniteInternalFuture acquireReadLock = GridTestUtils.runAsync(() -> {
            db.checkpointReadLock();
            db.checkpointReadUnlock();
        });

        assertTrue(hndLatch.await(5, TimeUnit.SECONDS));

        acquireWriteLock.cancel();

        acquireReadLock.get(5, TimeUnit.SECONDS);

        GridTestUtils.waitForCondition(acquireWriteLock::isCancelled, 5000);

        stopGrid(0);
    }
}
