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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.incrementalSnapshotMetaFileName;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class IncrementalSnapshotTest extends AbstractSnapshotSelfTest {
    /** */
    @Test
    public void testCreation() throws Exception {
        IgniteEx ign = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        snp(ign).createSnapshot(SNAPSHOT_NAME).get();

        for (int idx = 1; idx < 3; idx++) {
            snp(ign).createIncrementalSnapshot(SNAPSHOT_NAME).get();

            File incSnpDir = snp(ign).incrementalSnapshotLocalDir(SNAPSHOT_NAME, null, idx);

            assertTrue(incSnpDir.exists());
            assertTrue(incSnpDir.isDirectory());
            assertTrue(new File(incSnpDir, incrementalSnapshotMetaFileName(idx)).exists());
        }
    }

    /** */
    @Test
    public void testFailForUnknownBaseSnapshot() throws Exception {
        IgniteEx ign = startGridsWithCache(1, CACHE_KEYS_RANGE, key -> new Account(key, key),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        assertThrowsWithCause(
            () -> snp(ign).createIncrementalSnapshot("unknown").get(),
            IgniteException.class
        );

        snp(ign).createSnapshot(SNAPSHOT_NAME).get();

        assertThrowsWithCause(
            () -> snp(ign).createIncrementalSnapshot("unknown").get(),
            IgniteException.class
        );
    }
}
