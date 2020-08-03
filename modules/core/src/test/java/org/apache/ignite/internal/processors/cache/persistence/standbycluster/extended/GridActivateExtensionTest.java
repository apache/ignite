/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.standbycluster.extended;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearOnlyMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 *
 */
public class GridActivateExtensionTest extends GridCacheAbstractFullApiSelfTest {
    /** Condition id. */
    private int condId;

    /** Primary ip finder. */
    private TcpDiscoveryIpFinder primaryIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** Back up ip finder. */
    private TcpDiscoveryIpFinder backUpIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** Back up cluster. */
    private static Map<String, String> backUpCluster = new LinkedHashMap<>();

    /** Test name. */
    private final String testName = testName();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId("ConsId" + (condId++));
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(primaryIpFinder);

        DataStorageConfiguration pCfg = new DataStorageConfiguration();

        pCfg.setStoragePath(testName + "/db");
        pCfg.setWalArchivePath(testName + "/db/wal/archive");
        pCfg.setWalPath(testName + "/db/wal");

        pCfg.setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setMaxSize(200L * 1024 * 1024).setPersistenceEnabled(true));

        pCfg.setWalMode(WALMode.LOG_ONLY);

        pCfg.setPageSize(1024);
        pCfg.setConcurrencyLevel(64);

        cfg.setDataStorageConfiguration(pCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName, true));

        super.beforeTestsStarted();

        if (onlyClient())
            return;

        int nodes = gridCount();

        if (nodes != condId)
            fail("checking fail");

        condId = 0;

        log.info("start backup cluster");

        for (String name: backUpCluster.keySet()) {
            String n = name + "backUp";

            IgniteConfiguration cfg = getConfiguration(n);
            cfg.setActiveOnStart(false);
            ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(backUpIpFinder);

            startGrid(n, cfg);

            backUpCluster.put(name, n);
        }

        log.info("shutdown main cluster");

        for (String name : backUpCluster.keySet()) {
            log.info(name + " stopping");

            stopGrid(name);
        }

        Ignite ig = grid(0);

        ig.active(true);

        log.info("activate backUp cluster");

        assertEquals(true, ig.active());

        Class c = getClass();

        if (isNearTest(c)) {
            for (int i = 0; i < gridCount(); i++) {
                if (ignite(i).configuration().isClientMode()) {
                    Method m = getClientHasNearCacheMethod();

                    Boolean r = (Boolean)m.invoke(this);

                    if (r != null && r)
                        ignite(i).createNearCache(null, new NearCacheConfiguration<>());
                    else
                        ignite(i).cache(null);

                    break;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        backUpCluster.clear();

        condId = 0;

        U.delete(U.resolveWorkDirectory(U.defaultWorkDirectory(), testName, true));
    }

    /** {@inheritDoc} */
    @Override protected Ignite startGrid(String gridName, GridSpringResourceContext ctx) throws Exception {
        if (!gridName.contains("backUp"))
            backUpCluster.put(gridName, gridName);

        return super.startGrid(gridName, ctx);
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx grid(String name) {
        String n = name;

        if (backUpCluster.containsKey(name))
            n = backUpCluster.get(name);

        return super.grid(n);
    }

    /**
     *
     */
    public boolean onlyClient() {
        for (int i = 0; i < gridCount(); i++)
            if (!grid(i).configuration().isClientMode())
                return false;

        return true;
    }

    /**
     * @param c class.
     */
    private boolean isNearTest(Class c) {
        Class p = c.getSuperclass();

        while (!p.equals(Object.class)) {
            if (p.equals(GridCacheNearOnlyMultiNodeFullApiSelfTest.class))
                return true;

            p = p.getSuperclass();
        }

        return false;
    }

    /**
     *
     */
    public Method getClientHasNearCacheMethod() throws NoSuchMethodException {
        Class p = getClass().getSuperclass();

        while (!p.equals(Object.class)) {
            if (p.equals(GridCacheNearOnlyMultiNodeFullApiSelfTest.class))
                return p.getDeclaredMethod("clientHasNearCache");

            p = p.getSuperclass();
        }

        return null;
    }

    /**
     *
     */
    protected String testName() {
        return getClass().getSimpleName();
    }
}
