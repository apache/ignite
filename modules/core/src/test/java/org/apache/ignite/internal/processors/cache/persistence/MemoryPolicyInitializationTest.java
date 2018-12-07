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
package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Collection;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.configuration.MemoryConfiguration.DFLT_MEM_PLC_DEFAULT_NAME;

/**
 *
 */
public class MemoryPolicyInitializationTest extends GridCommonAbstractTest {
    /** */
    private static final String CUSTOM_NON_DEFAULT_MEM_PLC_NAME = "custom_mem_plc";

    /** */
    private static final long USER_CUSTOM_MEM_PLC_SIZE = 89L * 1024 * 1024;

    /** */
    private static final long USER_DEFAULT_MEM_PLC_SIZE = 99L * 1024 * 1024;

    /** */
    private MemoryConfiguration memCfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMemoryConfiguration(memCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        memCfg = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Verifies that expected memory policies are allocated when used doesn't provide any MemoryPolicyConfiguration.
     */
    public void testNoConfigProvided() throws Exception {
        memCfg = null;

        IgniteEx ignite = startGrid(0);

        Collection<DataRegion> allMemPlcs = ignite.context().cache().context().database().dataRegions();

        assertEquals(3, allMemPlcs.size());

        verifyDefaultAndSystemMemoryPolicies(allMemPlcs);
    }

    /**
     * Verifies that expected memory policies are allocated when used provides MemoryPolicyConfiguration
     * with non-default custom MemoryPolicy.
     */
    public void testCustomConfigNoDefault() throws Exception {
        prepareCustomNoDefaultConfig();

        IgniteEx ignite = startGrid(0);

        Collection<DataRegion> allMemPlcs = ignite.context().cache().context().database().dataRegions();

        assertEquals(4, allMemPlcs.size());

        verifyDefaultAndSystemMemoryPolicies(allMemPlcs);

        assertTrue("Custom non-default memory policy is not presented",
                isMemoryPolicyPresented(allMemPlcs, CUSTOM_NON_DEFAULT_MEM_PLC_NAME));
    }

    /**
     * User is allowed to configure memory policy with 'default' name,
     * in that case Ignite instance will use this user-defined memory policy as a default one.
     */
    public void testCustomConfigOverridesDefault() throws Exception {
        prepareCustomConfigWithOverridingDefault();

        IgniteEx ignite = startGrid(0);

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        Collection<DataRegion> allMemPlcs = dbMgr.dataRegions();

        assertEquals(3, allMemPlcs.size());

        verifyDefaultAndSystemMemoryPolicies(allMemPlcs);

        DataRegion dfltMemPlc = U.field(dbMgr, "dfltDataRegion");

        assertEquals(dfltMemPlc.config().getMaxSize(), USER_DEFAULT_MEM_PLC_SIZE);
    }

    /**
     * User is allowed to define fully custom memory policy and make it default by setting its name to memory config.
     *
     * At the same time user still can create a memory policy with name 'default'
     * which although won't be used as default.
     */
    public void testCustomConfigOverridesDefaultNameAndDeclaresDefault() throws Exception {
        prepareCustomConfigWithOverriddenDefaultName();

        IgniteEx ignite = startGrid(0);

        IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

        Collection<DataRegion> allMemPlcs = dbMgr.dataRegions();

        assertEquals(4, allMemPlcs.size());

        verifyDefaultAndSystemMemoryPolicies(allMemPlcs);

        DataRegion dfltMemPlc = U.field(dbMgr, "dfltDataRegion");

        assertEquals(dfltMemPlc.config().getMaxSize(), USER_CUSTOM_MEM_PLC_SIZE);
    }

    /**
     * Test for verification that caches with not specified memory policy name,
     * with specified default memory policy name and specified custom memory policy name
     * all started with correct memory policy.
     */
    public void testCachesOnOverriddenMemoryPolicy() throws Exception {
        prepareCustomConfigWithOverridingDefaultAndCustom();

        IgniteEx ignite = startGrid(0);

        CacheConfiguration cache1Cfg = new CacheConfiguration()
                .setName("cache1");

        IgniteCache cache1 = ignite.createCache(cache1Cfg);

        verifyCacheMemoryPolicy(cache1, DFLT_MEM_PLC_DEFAULT_NAME);

        CacheConfiguration cache2Cfg = new CacheConfiguration()
                .setName("cache2")
                .setMemoryPolicyName(CUSTOM_NON_DEFAULT_MEM_PLC_NAME);

        IgniteCache cache2 = ignite.createCache(cache2Cfg);

        verifyCacheMemoryPolicy(cache2, CUSTOM_NON_DEFAULT_MEM_PLC_NAME);

        CacheConfiguration cache3Cfg = new CacheConfiguration()
                .setName("cache3")
                .setMemoryPolicyName(DFLT_MEM_PLC_DEFAULT_NAME);

        IgniteCache cache3 = ignite.createCache(cache3Cfg);

        verifyCacheMemoryPolicy(cache3, DFLT_MEM_PLC_DEFAULT_NAME);
    }

    /**
     * Test for verification that caches with not specified memory policy name,
     * with specified default memory policy name and specified custom memory policy name
     * all started with correct memory policy.
     */
    public void testCachesOnUserDefinedDefaultMemoryPolicy() throws Exception {
        prepareCustomConfigWithOverriddenDefaultName();

        IgniteEx ignite = startGrid(0);

        CacheConfiguration cache1Cfg = new CacheConfiguration()
                .setName("cache1");

        IgniteCache cache1 = ignite.createCache(cache1Cfg);

        verifyCacheMemoryPolicy(cache1, CUSTOM_NON_DEFAULT_MEM_PLC_NAME);

        CacheConfiguration cache2Cfg = new CacheConfiguration()
                .setName("cache2")
                .setMemoryPolicyName(CUSTOM_NON_DEFAULT_MEM_PLC_NAME);

        IgniteCache cache2 = ignite.createCache(cache2Cfg);

        verifyCacheMemoryPolicy(cache2, CUSTOM_NON_DEFAULT_MEM_PLC_NAME);

        CacheConfiguration cache3Cfg = new CacheConfiguration()
                .setName("cache3")
                .setMemoryPolicyName(DFLT_MEM_PLC_DEFAULT_NAME);

        IgniteCache cache3 = ignite.createCache(cache3Cfg);

        verifyCacheMemoryPolicy(cache3, DFLT_MEM_PLC_DEFAULT_NAME);
    }

    /**
     * @param cache Cache.
     * @param plcName Policy name.
     */
    private void verifyCacheMemoryPolicy(IgniteCache cache, String plcName) {
        GridCacheContext ctx = ((IgniteCacheProxy) cache).context();

        assertEquals(plcName, ctx.dataRegion().config().getName());
    }

    /**
     *
     */
    private void prepareCustomConfigWithOverriddenDefaultName() {
        memCfg = new MemoryConfiguration();

        memCfg.setDefaultMemoryPolicyName(CUSTOM_NON_DEFAULT_MEM_PLC_NAME);

        memCfg.setMemoryPolicies(new MemoryPolicyConfiguration()
                .setName(CUSTOM_NON_DEFAULT_MEM_PLC_NAME)
                .setInitialSize(USER_CUSTOM_MEM_PLC_SIZE)
                .setMaxSize(USER_CUSTOM_MEM_PLC_SIZE),

            new MemoryPolicyConfiguration()
                .setName(DFLT_MEM_PLC_DEFAULT_NAME)
                .setInitialSize(USER_CUSTOM_MEM_PLC_SIZE)
                .setMaxSize(USER_DEFAULT_MEM_PLC_SIZE)
        );
    }


    /**
     *
     */
    private void prepareCustomConfigWithOverridingDefault() {
        memCfg = new MemoryConfiguration();

        memCfg.setMemoryPolicies(new MemoryPolicyConfiguration()
            .setName(DFLT_MEM_PLC_DEFAULT_NAME)
            .setInitialSize(USER_CUSTOM_MEM_PLC_SIZE)
            .setMaxSize(USER_DEFAULT_MEM_PLC_SIZE)
        );
    }

    /**
     *
     */
    private void prepareCustomConfigWithOverridingDefaultAndCustom() {
        memCfg = new MemoryConfiguration();

        memCfg.setMemoryPolicies(new MemoryPolicyConfiguration()
                .setName(DFLT_MEM_PLC_DEFAULT_NAME)
                .setInitialSize(USER_CUSTOM_MEM_PLC_SIZE)
                .setMaxSize(USER_DEFAULT_MEM_PLC_SIZE),

            new MemoryPolicyConfiguration()
                .setName(CUSTOM_NON_DEFAULT_MEM_PLC_NAME)
                .setInitialSize(USER_CUSTOM_MEM_PLC_SIZE)
                .setMaxSize(USER_CUSTOM_MEM_PLC_SIZE)
        );
    }

    /**
     * @param allMemPlcs Collection of all memory policies.
     */
    private void verifyDefaultAndSystemMemoryPolicies(Collection<DataRegion> allMemPlcs) {
        assertTrue("Default memory policy is not presented",
                isMemoryPolicyPresented(allMemPlcs, DFLT_MEM_PLC_DEFAULT_NAME));

        assertTrue("System memory policy is not presented",
                isMemoryPolicyPresented(allMemPlcs, IgniteCacheDatabaseSharedManager.SYSTEM_DATA_REGION_NAME));
    }

    /**
     *
     */
    private void prepareCustomNoDefaultConfig() {
        memCfg = new MemoryConfiguration();

        memCfg.setMemoryPolicies(new MemoryPolicyConfiguration()
            .setName(CUSTOM_NON_DEFAULT_MEM_PLC_NAME)
            .setInitialSize(USER_CUSTOM_MEM_PLC_SIZE)
            .setMaxSize(USER_CUSTOM_MEM_PLC_SIZE)
        );
    }

    /**
     * @param memPlcs Collection of memory policies.
     * @param nameToVerify Excepted name of memory policy.
     */
    private boolean isMemoryPolicyPresented(Collection<DataRegion> memPlcs, String nameToVerify) {
        for (DataRegion memPlc : memPlcs) {
            if (nameToVerify.equals(memPlc.config().getName()))
                return true;
        }

        return false;
    }
}
