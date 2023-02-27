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

package org.apache.ignite.internal.processors.security.snapshot;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.AbstractTestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.lang.String.format;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_SNAPSHOT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Snapshot execute permission tests.
 */
@RunWith(JUnit4.class)
public class SnapshotPermissionCheckTest extends AbstractSecurityTest {
    /** Test snapshot name. */
    private static final String SNAPSHOT_NAME = "security_snapshot_%s";

    /** Reentrant lock. */
    private static final ReentrantLock RNT_LOCK = new ReentrantLock();

    /** Snapshot atomic sequence. */
    private final AtomicInteger snpCntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotAllowed() throws Exception {
        doTest(this::run, ADMIN_SNAPSHOT);
    }

    /** @throws Exception If fails. */
    @Test
    public void testSnapshotForbidden() throws Exception {
        doTest(op -> assertThrowsWithCause(() -> run(op), SecurityException.class), EMPTY_PERMS);
    }

    /**
     * @param check Check condition.
     * @param perms Permissions to check.
     * @throws Exception If fails.
     */
    private void doTest(Consumer<? super Supplier<Future<Void>>> check, SecurityPermission... perms) throws Exception {
        Ignite srv = startGrid("srv", permissions(F.concat(perms, ADMIN_CLUSTER_STATE)), false);

        Ignite clnt = startGrid("clnt", permissions(perms), true);

        srv.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int idx = 0; idx < 1024; idx++)
            cache.put(idx, -1);

        forceCheckpoint();

        asyncOperations(srv, clnt).forEach(check);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName,
        AbstractTestSecurityPluginProvider pluginProv) throws Exception {
        return super.getConfiguration(instanceName, pluginProv)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));
    }

    /** */
    private Stream<Supplier<Future<Void>>> asyncOperations(Ignite... nodes) {
        Function<Ignite, Stream<Supplier<Future<Void>>>> nodeOps = (node) -> Stream.of(
            () -> new TestFutureAdapter<>(node.snapshot().createSnapshot(format(SNAPSHOT_NAME, snpCntr.getAndIncrement()))),
            () -> new TestFutureAdapter<>(node.snapshot().cancelSnapshot(format(SNAPSHOT_NAME, snpCntr.getAndIncrement())))
        );

        return Arrays.stream(nodes).flatMap(nodeOps);
    }

    /**
     * @param perms Permissions.
     */
    private static SecurityPermissionSet permissions(SecurityPermission... perms) {
        return SecurityPermissionSetBuilder.create()
            .defaultAllowAll(false)
            .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_CREATE, CACHE_PUT)
            .appendSystemPermissions(perms)
            .build();
    }

    /**
     * @param sup Supplier.
     */
    private void run(Supplier<Future<Void>> sup) {
        RNT_LOCK.lock();

        try {
            sup.get().get();
        }
        catch (InterruptedException | ExecutionException e) {
            throw new IgniteException(e);
        }
        finally {
            RNT_LOCK.unlock();
        }
    }
}
