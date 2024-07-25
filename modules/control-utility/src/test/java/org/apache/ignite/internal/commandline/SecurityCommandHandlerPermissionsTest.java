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

package org.apache.ignite.internal.commandline;

import java.security.Permissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.processors.dr.GridDrType;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.junit.Test;
import org.junit.runners.Parameterized;

import static java.util.Arrays.asList;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_PASSWORD;
import static org.apache.ignite.internal.commandline.ArgumentParser.CMD_USER;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_DESTROY;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALL_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.systemPermissions;

/** */
public class SecurityCommandHandlerPermissionsTest extends GridCommandHandlerAbstractTest {
    /** */
    private static final String TEST_NO_PERMISSIONS_LOGIN = "no-permissions-login";

    /** */
    private static final String TEST_LOGIN = "cli-admin-login";

    /** */
    private static final String DEFAULT_PWD = "pwd";

    /** */
    private static final int PARTITIONS = 8;

    /** */
    @Parameterized.Parameters(name = "cmdHnd={0}")
    public static List<String> commandHandlers() {
        return Collections.singletonList(CLI_CMD_HND);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        persistenceEnable(false);

        injectTestSystemOut();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testClusterTag() throws Exception {
        Ignite ignite = startGrid(0, userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS));

        assertEquals(
            EXIT_CODE_OK,
            execute(enrichWithConnectionArguments(Collections.singleton("--state"), TEST_NO_PERMISSIONS_LOGIN))
        );

        String out = testOut.toString();

        UUID clId = ignite.cluster().id();
        String clTag = ignite.cluster().tag();

        assertTrue(out.contains("Cluster  ID: " + clId));
        assertTrue(out.contains("Cluster tag: " + clTag));
    }

    /** */
    @Test
    public void testCacheScan() throws Exception {
        checkCommandPermissions(asList("--cache", "scan", DEFAULT_CACHE_NAME), cachePermission(CACHE_READ));
    }

    /** */
    @Test
    public void testCacheDestroy() throws Exception {
        checkCommandPermissions(asList("--cache", "destroy", "--caches", DEFAULT_CACHE_NAME), systemPermissions(CACHE_DESTROY));
    }

    /** */
    @Test
    public void testCacheClear() throws Exception {
        checkCommandPermissions(asList("--cache", "clear", "--caches", DEFAULT_CACHE_NAME), cachePermission(CACHE_REMOVE));
    }

    /** */
    @Test
    public void testConsistencyRepair() throws Exception {
        List<String> cmdArgs = asList(
            "--consistency", "repair",
            "--cache", DEFAULT_CACHE_NAME,
            "--strategy", "LWW",
            "--partitions", IntStream.range(0, PARTITIONS).mapToObj(Integer::toString).collect(Collectors.joining(","))
        );

        checkConsistencyCommandPermissions(cmdArgs, adminPermission(ADMIN_OPS));
    }

    /** */
    @Test
    public void testCacheCreate() throws Exception {
        String ccfgPath = resolveIgnitePath(
            "modules/control-utility/src/test/resources/config/cache/cache-create-correct.xml"
        ).getAbsolutePath();

        checkCommandPermissions(
            asList("--cache", "create", "--springxmlconfig", ccfgPath),
            systemPermissions(CACHE_CREATE)
        );
    }

    /** */
    protected IgniteEx startGrid(int idx, TestSecurityData... userData) throws Exception {
        String login = getTestIgniteInstanceName(idx);

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(idx));

        cfg.setPluginProviders(new TestSecurityPluginProvider(login, DEFAULT_PWD, ALL_PERMISSIONS, false, userData));

        return startGrid(cfg);
    }

    /** */
    private void checkCommandPermissions(Collection<String> cmdArgs, SecurityPermissionSet reqPerms) throws Exception {
        Ignite ignite = startNode(0, reqPerms);

        ignite.createCache(DEFAULT_CACHE_NAME);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(enrichWithConnectionArguments(cmdArgs, TEST_NO_PERMISSIONS_LOGIN)));

        // We are losing command failure cause for --cache clear commnad. See IGNITE-21023 for more details.
        if (!cmdArgs.containsAll(Arrays.asList("--cache", "clear")))
            assertTrue(testOut.toString().contains("Authorization failed"));

        assertEquals(EXIT_CODE_OK, execute(enrichWithConnectionArguments(cmdArgs, TEST_LOGIN)));
    }

    /** */
    private void checkConsistencyCommandPermissions(Collection<String> cmdArgs, SecurityPermissionSet reqPerms) throws Exception {
        int nodes = 3;

        Ignite ignite = startGrid(nodes, reqPerms);

        String cacheName = createCache(nodes, ignite);

        fillCache(cacheName);

        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute(enrichWithConnectionArguments(cmdArgs, TEST_NO_PERMISSIONS_LOGIN)));

        // We are losing command failure cause for --cache clear commnad. See IGNITE-21023 for more details.
        if (!cmdArgs.containsAll(Arrays.asList("--cache", "clear")))
            assertTrue(testOut.toString().contains("Authorization failed"));

        assertEquals(EXIT_CODE_OK, execute(enrichWithConnectionArguments(cmdArgs, TEST_LOGIN)));
    }

    /**
     * @param nodes Number of nodes in cluster.
     * @param reqPerms security permition set.
     * @return {@link Ignite} instance of the first node in the grid.
     */
    private Ignite startGrid(int nodes, SecurityPermissionSet reqPerms) throws Exception {
        assert nodes > 0;

        Ignite ignite = startNode(0, reqPerms);

        for(int i = 1; i < nodes; ++i)
            startNode(i, reqPerms);

        return ignite;
    }

    /**
     * Starts a single node with specified user security parameters.
     * @param idx node index.
     * @param reqPerms {@link SecurityPermissionSet} with permissions.
     */
    private Ignite startNode(int idx, SecurityPermissionSet reqPerms) throws Exception {
        return startGrid(
            idx,
            userData(TEST_NO_PERMISSIONS_LOGIN, NO_PERMISSIONS),
            userData(TEST_LOGIN, reqPerms)
        );
    }

    /**
     * @param ignite Ignite instance.
     * @param nodes Number for ignite instances.
     */
    private String createCache(int nodes, Ignite ignite) {
        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setBackups(nodes - 1);
        cfg.setAffinity(new RendezvousAffinityFunction().setPartitions(PARTITIONS));

        return ignite.getOrCreateCache(cfg).getName();
    }

    /** */
    List<String> enrichWithConnectionArguments(Collection<String> cmdArgs, String login) {
        List<String> args = new ArrayList<>();

        args.add(CMD_USER);
        args.add(login);
        args.add(CMD_PASSWORD);
        args.add(DEFAULT_PWD);

        args.addAll(cmdArgs);

        return args;
    }

    /** */
    private SecurityPermissionSet cachePermission(SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendCachePermissions(DEFAULT_CACHE_NAME, perms)
            .build();
    }

    /** */
    private SecurityPermissionSet adminPermission(SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendSystemPermissions(perms)
            .build();
    }

    /** */
    private TestSecurityData userData(String login, SecurityPermissionSet perms) {
        return new TestSecurityData(
            login,
            DEFAULT_PWD,
            perms,
            new Permissions()
        );
    }

    /**
     *
     */
    private void fillCache(String name) throws Exception {
        for (Ignite node : G.allGrids()) {
            while (((IgniteEx)node).cachex(name) == null) // Waiting for cache internals to init.
                U.sleep(1);
        }

        GridCacheVersionManager mgr =
            ((GridCacheAdapter)(grid(1)).cachex(name).cache()).context().shared().versions();

        for (int key = 0; key < PARTITIONS; key++) {
            List<Ignite> nodes = new ArrayList<>();

            nodes.add(primaryNode(key, name));
            nodes.addAll(backupNodes(key, name));

            Collections.shuffle(nodes);

            int val = key;
            Object obj;

            for (Ignite node : nodes) {
                IgniteInternalCache cache = ((IgniteEx)node).cachex(name);

                GridCacheAdapter adapter = ((GridCacheAdapter)cache.cache());

                GridCacheEntryEx entry = adapter.entryEx(key);

                obj = ++val;

                boolean init = entry.initialValue(
                    new CacheObjectImpl(obj, null), // Incremental or same value.
                    mgr.next(entry.context().kernalContext().discovery().topologyVersion()), // Incremental version.
                    0,
                    0,
                    false,
                    AffinityTopologyVersion.NONE,
                    GridDrType.DR_NONE,
                    false,
                    false);

                assertTrue("iterableKey " + key + " already inited", init);
            }
        }
    }
}
