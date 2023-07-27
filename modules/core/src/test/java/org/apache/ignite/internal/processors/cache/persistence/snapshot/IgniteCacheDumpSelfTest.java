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

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.IntStream;
import javax.management.DynamicMBean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.platform.model.ACL;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.Role;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.platform.model.Value;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.management.api.CommandMBean.INVOKE;
import static org.apache.ignite.platform.model.AccessLevel.SUPER;

/** */
public class IgniteCacheDumpSelfTest extends GridCommonAbstractTest {
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
        try (IgniteEx ign = startGrid(0)) {
            ign.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Object, Object> cache = ign.createCache(DEFAULT_CACHE_NAME);
            IgniteCache<Object, Object> grpCache0 = ign.createCache(
                new CacheConfiguration<>().setGroupName("grp").setName("cache-0")
            );
            IgniteCache<Object, Object> grpCache1 = ign.createCache(
                new CacheConfiguration<>().setGroupName("grp").setName("cache-1")
            );

            IntStream.range(0, 10).forEach(i -> {
                cache.put(i, i);
                grpCache0.put(i, new User(i, ACL.values()[i % ACL.values().length], new Role("Role" + i, SUPER)));
                grpCache1.put(new Key(i), new Value(String.valueOf(i)));
            });

            Object[] args = {"dump", ""};

            String[] signature = new String[args.length];

            Arrays.fill(signature, String.class.getName());

            String res = (String)createDumpBean(ign).invoke(INVOKE, args, signature);

            assertTrue(res.isEmpty());
        }
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
