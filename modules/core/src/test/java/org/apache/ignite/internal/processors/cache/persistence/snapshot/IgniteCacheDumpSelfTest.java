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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import javax.management.DynamicMBean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpEntry;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpIterator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.model.ACL;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.Role;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.platform.model.Value;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.management.api.CommandMBean.INVOKE;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_DUMPS_DIRECTORY;
import static org.apache.ignite.platform.model.AccessLevel.SUPER;

/** */
public class IgniteCacheDumpSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String GRP = "grp";

    /** */
    public static final String CACHE_0 = "cache-0";

    /** */
    public static final String CACHE_1 = "cache-1";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true))
        );
    }

    /** */
    @Test
    public void testCacheDump() throws Exception {
        IntFunction<User> userFactory = i ->
            new User(i, ACL.values()[i % ACL.values().length], new Role("Role" + i, SUPER));

        try (IgniteEx ign = startGrid(0)) {
            ign.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Object, Object> cache = ign.createCache(DEFAULT_CACHE_NAME);
            IgniteCache<Object, Object> grpCache0 = ign.createCache(
                new CacheConfiguration<>().setGroupName(GRP).setName(CACHE_0)
            );
            IgniteCache<Object, Object> grpCache1 = ign.createCache(
                new CacheConfiguration<>().setGroupName(GRP).setName(CACHE_1)
            );

            IntStream.range(0, 10).forEach(i -> {
                cache.put(i, i);
                grpCache0.put(i, userFactory.apply(i));
                grpCache1.put(new Key(i), new Value(String.valueOf(i)));
            });

            Object[] args = {"dump", ""};

            String[] signature = new String[args.length];

            Arrays.fill(signature, String.class.getName());

            String res = (String)createDumpBean(ign).invoke(INVOKE, args, signature);

            assertTrue(res.isEmpty());

            Dump dump = new Dump(
                ign.context(),
                new File(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_DUMPS_DIRECTORY, false), "dump")
            );

            assertNotNull(dump);

            SnapshotMetadata metadata = dump.metadata();

            assertNotNull(metadata);
            assertEquals("dump", metadata.snapshotName());
            assertTrue(metadata.dump());

            List<CacheConfiguration<?, ?>> ccfgs = dump.config(CU.cacheId(DEFAULT_CACHE_NAME));

            assertNotNull(ccfgs);
            assertEquals(1, ccfgs.size());
            assertEquals(DEFAULT_CACHE_NAME, ccfgs.get(0).getName());

            ccfgs = dump.config(CU.cacheId(GRP));

            assertNotNull(ccfgs);
            assertEquals(2, ccfgs.size());

            ccfgs.sort(Comparator.comparing(CacheConfiguration::getName));

            assertEquals(GRP, ccfgs.get(0).getGroupName());
            assertEquals(CACHE_0, ccfgs.get(0).getName());
            assertEquals(GRP, ccfgs.get(1).getGroupName());
            assertEquals(CACHE_1, ccfgs.get(1).getName());

            int cnt = 0;

            CacheObjectContext coCtx = ign.context().cache().context().cacheObjectContext(CU.cacheId(DEFAULT_CACHE_NAME));

            try (DumpIterator iter = dump.iterator(CU.cacheId(DEFAULT_CACHE_NAME))) {
                while (iter.hasNext()) {
                    DumpEntry e = iter.next();

                    assertNotNull(e);
                    assertEquals((Integer)cnt, e.key().value(coCtx, true));
                    assertEquals((Integer)cnt, e.value().value(coCtx, true));

                    cnt++;
                }
            }

            assertEquals(10, cnt);

            cnt = 0;

            CacheObjectContext coCtx0 = ign.context().cache().context().cacheObjectContext(CU.cacheId(CACHE_0));
            CacheObjectContext coCtx1 = ign.context().cache().context().cacheObjectContext(CU.cacheId(CACHE_1));

            try (DumpIterator iter = dump.iterator(CU.cacheId(GRP))) {
                while (iter.hasNext()) {
                    DumpEntry e = iter.next();

                    assertNotNull(e);

                    if (e.cacheId() == CU.cacheId(CACHE_0))
                        assertEquals(userFactory.apply(e.key().value(coCtx0, true)), e.value().value(coCtx0, true));

                    else {
                        assertNotNull(e.key().<Key>value(coCtx1, true));
                        assertNotNull(e.value().<Value>value(coCtx1, true));
                    }

                    cnt++;
                }
            }

            assertEquals(20, cnt);
        }
    }

    /** */
    @Test
    public void testCacheDumpWithConcurrentUpdates() throws Exception {

    }

    /** */
    private static DynamicMBean createDumpBean(IgniteEx ign) {
        DynamicMBean mbean = getMxBean(
            ign.context().igniteInstanceName(),
            "management",
            Collections.singletonList("Dump"),
            "Create",
            DynamicMBean.class
        );

        assertNotNull(mbean);

        return mbean;
    }
}
