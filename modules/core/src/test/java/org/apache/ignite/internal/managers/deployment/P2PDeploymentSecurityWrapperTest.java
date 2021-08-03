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

package org.apache.ignite.internal.managers.deployment;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.SecurityAwarePredicate;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_PUT;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests deployment remote filter with a security wrapper.
 */
public class P2PDeploymentSecurityWrapperTest extends AbstractSecurityTest {
    /** Server node. */
    private static final String SRV = "srv";

    /** Client node. */
    private static final String CLNT = "clnt";

    /** */
    private static final AtomicInteger locCntr = new AtomicInteger();

    /** */
    private static final AtomicInteger rmtCntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        return super.getConfiguration(instanceName).setIncludeEventTypes(EVT_CACHE_OBJECT_PUT);
    }

    /** */
    @Test
    public void testDeployment() throws Exception {
        IgniteEx srv = startGrid(SRV, ALLOW_ALL, false);

        UUID nodeId = srv.localNode().id();

        GridKernalContext ctx = srv.context();

        SecurityAwarePredicate<Event> securityRemFilter = new SecurityAwarePredicate<>(nodeId, (e) -> true );

        Class<?> aCls = securityRemFilter.getClass();

        GridDeployment dep = ctx.deploy().deploy(aCls, U.detectClassLoader(aCls));

        GridDeploymentInfoBean depInfo = new GridDeploymentInfoBean(dep);

        GridDeployment deployment = srv.context().deploy().getGlobalDeployment(DeploymentMode.SHARED, aCls.getName(), aCls.getName(),
            depInfo.userVersion(), nodeId, depInfo.classLoaderId(), depInfo.participants(), null);

        assertFalse(deployment.local());
    }

    /** */
    @Test
    public void testListen() throws Exception {
        startGrid(SRV, ALLOW_ALL, false);
        IgniteEx clnt = startGrid(CLNT, ALLOW_ALL, true);

        clnt.events().remoteListen(
            (uuid, evt) -> {
            locCntr.incrementAndGet();

            return true;
        },
            (evt) -> {
            rmtCntr.incrementAndGet();

            return true;
        },
            EVT_CACHE_OBJECT_PUT);

        check(clnt);
    }

    /** */
    @Test
    public void testListenAcync() throws Exception {
        startGrid(SRV, ALLOW_ALL, false);
        IgniteEx clnt = startGrid(CLNT, ALLOW_ALL, true);

        clnt.events().remoteListenAsync(
            (uuid, evt) -> {
                locCntr.incrementAndGet();

                return true;
            },
            (evt) -> {
                rmtCntr.incrementAndGet();

                return true;
            },
            EVT_CACHE_OBJECT_PUT);

        check(clnt);
    }

    /** */
    private void check(Ignite ignite) throws IgniteInterruptedCheckedException {
        IgniteCache<Object, Object> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.clear();

        locCntr.set(0);
        rmtCntr.set(0);

        int size = 100;

        for (int i = 0; i < size; i++)
            cache.put(i, i);

        assertEquals(100, size);

        assertTrue(waitForCondition(() -> locCntr.get() == size, getTestTimeout()));
        assertTrue(waitForCondition(() -> rmtCntr.get() == size, getTestTimeout()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();

        super.afterTest();
    }
}
