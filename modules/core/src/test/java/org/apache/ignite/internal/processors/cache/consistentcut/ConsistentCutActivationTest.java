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

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cluster.ClusterState;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/** */
public class ConsistentCutActivationTest extends AbstractConsistentCutTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        try (IgniteDataStreamer<Integer, Integer> ds = grid(0).dataStreamer(CACHE)) {
            for (int i = 0; i < 10_000; i++)
                ds.addData(i, i);
        }
    }

    /** */
    @Test
    public void testConsistentCutDisabledByDefault() {
        assertConsistentCutDisabled(null);
    }

    /** */
    @Test
    public void testConsistentCutStartedOnSnapshotCreation() {
        grid(1).context().cache().context().snapshotMgr().createSnapshot("snp").get();

        assertConsistentCutEnabled();
    }

    /** */
    @Test
    public void testConsistentCutStartedOnSnapshotRestore() {
        grid(1).context().cache().context().snapshotMgr().createSnapshot("snp").get();

        TestConsistentCutManager.cutMgr(grid(0)).disableScheduling(false);

        assertConsistentCutDisabled(null);

        grid(0).cache(CACHE).destroy();

        grid(1).context().cache().context().snapshotMgr().restoreSnapshot("snp", null).get();

        assertConsistentCutEnabled();
    }

    /** */
    @Test
    public void testConsistentCutStoppedOnServerFailed() {
        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();

        assertConsistentCutEnabled();

        stopGrid(1);

        assertConsistentCutDisabled(1);
    }

    /** */
    @Test
    public void testConsistentCutStoppedOnServerAdd() throws Exception {
        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();

        assertConsistentCutEnabled();

        startGrid(nodes() + 1);

        assertConsistentCutDisabled(null);
    }

    /** */
    @Test
    public void testConsistentCutClientNotAffect() throws Exception {
        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();

        assertConsistentCutEnabled();

        startClientGrid(nodes() + 1);

        assertConsistentCutEnabled();

        stopGrid(nodes());

        assertConsistentCutEnabled();
    }

    /** */
    @Test
    public void testClusterActivationNotAffect() throws Exception {
        TestConsistentCutManager.cutMgr(grid(0)).scheduleConsistentCut();

        assertConsistentCutEnabled();

        grid(0).cluster().state(ClusterState.INACTIVE);

        long actVer = awaitGlobalCutReady(2, false);

        assertConsistentCutEnabled();

        assertTrue(actVer >= 2);

        grid(0).cluster().state(ClusterState.ACTIVE);

        awaitGlobalCutReady(actVer + 1, false);

        assertConsistentCutEnabled();
    }

    /** */
    private void assertConsistentCutEnabled() {
        assertFalse(TestConsistentCutManager.disabled(grid(0)));

        for (int i = 1; i < nodes(); i++)
            assertTrue(TestConsistentCutManager.disabled(grid(i)));
    }

    /** */
    private void assertConsistentCutDisabled(@Nullable Integer excl) {
        for (int i = 0; i < nodes(); i++) {
            if (excl != null && excl == i)
                continue;

            assertTrue(TestConsistentCutManager.disabled(grid(i)));
        }
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
