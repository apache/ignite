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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;
import java.net.URL;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_METASTORAGE_KEYS_TO_SKIP;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests the metastorage keys skip.
 *
 * @see IgniteSystemProperties#IGNITE_METASTORAGE_KEYS_TO_SKIP
 */
public class MetaStorageSkipKeysTest extends GridCommonAbstractTest {
    /** Test key 1. (For a value with unknown class after recovery. */
    private static final String KEY_1 = "test-unknown-class-key-1";

    /** Test value 1 classname. */
    private static final String VALUE_1_CLASSNAME = "org.apache.ignite.tests.p2p.TestUserResource";

    /** Test key 2. */
    private static final String KEY_2 = "test-key-2";

    /** Test value 2. */
    private static final String VALUE_2 = "test-value-2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
            )
        );

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testSkipKeys() throws Exception {
        IgniteEx ign = startGrid(0);
        ign.cluster().state(ClusterState.ACTIVE);

        assertTrue(!U.inClassPath(VALUE_1_CLASSNAME));

        // Write key1 and key2 to metastorage.
        IgniteCacheDatabaseSharedManager db = ign.context().cache().context().database();

        db.checkpointReadLock();

        try {
            GridTestExternalClassLoader ldr =
                new GridTestExternalClassLoader(new URL[] {new URL(GridTestProperties.getProperty("p2p.uri.cls"))});

            db.metaStorage().write(KEY_1,
                (Serializable)ldr.loadClass(VALUE_1_CLASSNAME).newInstance());
            db.metaStorage().write(KEY_2, VALUE_2);
        }
        finally {
            ign.context().cache().context().database().checkpointReadUnlock();
        }

        assertThrowsWithCause(() -> db.metaStorage().read(KEY_1), ClassNotFoundException.class);

        assertThrowsWithCause(() -> {
            try {
                db.metaStorage().iterate(KEY_1, (key, val) -> fail(), true);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }, ClassNotFoundException.class);

        stopAllGrids();

        try {
            System.setProperty(IGNITE_METASTORAGE_KEYS_TO_SKIP, KEY_1);

            ign = startGrid(0);
            ign.cluster().state(ClusterState.ACTIVE);

            MetaStorage metaStorage = ign.context().cache().context().database().metaStorage();

            metaStorage.iterate(KEY_1, (key, val) -> fail(), true);

            assertEquals(metaStorage.read(KEY_2), VALUE_2);

            stopAllGrids();

            System.setProperty(IGNITE_METASTORAGE_KEYS_TO_SKIP, KEY_1 + "," + KEY_2);

            ign = startGrid(0);

            ign.cluster().state(ClusterState.ACTIVE);

            metaStorage = ign.context().cache().context().database().metaStorage();

            metaStorage.iterate(KEY_1, (key, val) -> fail(), true);
            metaStorage.iterate(KEY_2, (key, val) -> fail(), true);
        }
        finally {
            System.clearProperty(IGNITE_METASTORAGE_KEYS_TO_SKIP);
        }
    }
}
