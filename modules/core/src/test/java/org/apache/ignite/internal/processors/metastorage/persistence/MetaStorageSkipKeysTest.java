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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_METASTORAGE_KEYS_TO_SKIP;

/**
 * Tests the distributed metastorage keys skip.
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

    /** */
    private int excludeKeys;

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

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean isRemoteJvm(String igniteInstanceName) {
        return igniteInstanceName.contains("1");
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmClasspath() {
        if (excludeKeys == 0)
            return Collections.singletonList(GridTestProperties.getProperty("p2p.uri.classpath"));
        else
            return Collections.emptyList();
    }

    @Override protected List<String> additionalRemoteJvmArgs() {
        if (excludeKeys == 0)
            return Collections.emptyList();
        else if (excludeKeys == 1)
            return Collections.singletonList("-D" + IGNITE_METASTORAGE_KEYS_TO_SKIP + "=" + KEY_1);
        else
            return Collections.singletonList("-D" + IGNITE_METASTORAGE_KEYS_TO_SKIP + "=" + KEY_1 + "," + KEY_2);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** @throws Exception If failed. */
    @Test
    public void testSkipKeys() throws Exception {
        excludeKeys = 0;

        IgniteEx ign = startGrid(0);

        // 1. Start remote JVM ignite instance and write to the metastorage a value with class.
        startGrid(1);

        ign.cluster().state(ClusterState.ACTIVE);

        int res = ign.compute(ign.cluster().forRemotes()).apply(new WriteToMetastorage(), 0);

        assertEquals(0, res);

        stopAllGrids();

        System.setProperty(IGNITE_METASTORAGE_KEYS_TO_SKIP, KEY_1);

        excludeKeys = 1;

        try {
            ign = startGrid(0);

            startGrid(1);

            ign.cluster().state(ClusterState.ACTIVE);

            // Check key1 excluded and key2 available.
            res = ign.compute(ign.cluster().forRemotes()).apply(new CheckMetastorage(), 0);

            assertEquals(0, res);
        }
        finally {
            System.clearProperty(IGNITE_METASTORAGE_KEYS_TO_SKIP);

            stopAllGrids();
        }

        System.setProperty(IGNITE_METASTORAGE_KEYS_TO_SKIP, KEY_1 + "," + KEY_2);

        excludeKeys = 2;

        try {
            ign = startGrid(0);

            // 3. Recovery metastorage from remote node data and check on local JVM.
            startGrid(1);

            ign.cluster().state(ClusterState.ACTIVE);

            // Check key1 and key2 excluded.
            res = ign.compute(ign.cluster().forRemotes()).apply(new CheckMetastorage2(), 0);

            assertEquals(0, res);
        }
        finally {
            System.clearProperty(IGNITE_METASTORAGE_KEYS_TO_SKIP);
        }
    }

    /** Job for a remote JVM Ignite instance to add event listener. */
    private static class WriteToMetastorage implements IgniteClosure<Integer, Integer> {
        /** Auto injected ignite instance. */
        @IgniteInstanceResource
        IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Integer apply(Integer i) {
            if (!U.inClassPath(VALUE_1_CLASSNAME))
                return -1;


            ignite.context().cache().context().database().checkpointReadLock();

            try {
                ignite.context().cache().context().database().metaStorage().write(KEY_1,
                    U.newInstance(VALUE_1_CLASSNAME));
                ignite.context().cache().context().database().metaStorage().write(KEY_2, VALUE_2);
            }
            catch (IgniteCheckedException e) {
                e.printStackTrace();

                return -2;
            } finally {
                ignite.context().cache().context().database().checkpointReadUnlock();
            }

            return 0;
        }
    }

    /** */
    private static class CheckMetastorage implements IgniteClosure<Integer, Integer> {
        /** Auto injected ignite instance. */
        @IgniteInstanceResource
        IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Integer apply(Integer i) {
            if ((U.inClassPath(VALUE_1_CLASSNAME)))
                return -1;

            MetaStorage metastorage = ignite.context().cache().context().database().metaStorage();

            try {
                final boolean[] res = {true};

                metastorage.iterate(KEY_1, (key, val) -> res[0] = false, false);

                if (!res[0])
                    return -2;

                if (!Objects.equals(metastorage.read(KEY_2), VALUE_2))
                    return -3;
            }
            catch (IgniteCheckedException e) {
                return -4;
            }

            return 0;
        }
    }

    /** */
    private static class CheckMetastorage2 implements IgniteClosure<Integer, Integer> {
        /** Auto injected ignite instance. */
        @IgniteInstanceResource
        IgniteEx ignite;

        /** {@inheritDoc} */
        @Override public Integer apply(Integer i) {
            if ((U.inClassPath(VALUE_1_CLASSNAME)))
                return -1;

            MetaStorage metastorage = ignite.context().cache().context().database().metaStorage();

            try {
                final boolean[] res = {true};

                metastorage.iterate(KEY_1, (key, val) -> res[0] = false, false);

                if (!res[0])
                    return -2;

                metastorage.iterate(KEY_2, (key, val) -> res[0] = false, false);

                if (!res[0])
                    return -3;
            }
            catch (IgniteCheckedException e) {
                return -4;
            }

            return 0;
        }
    }
}
