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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cluster.IgniteChangeGlobalStateSupport;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/** */
@RunWith(Parameterized.class)
public class ClusterActivationFailureTest extends GridCommonAbstractTest {
    /** */
    private static final int TEST_NODES_CNT = 3;

    /** */
    private final TestPluginProvider plugin = new TestPluginProvider();

    /** */
    @Parameterized.Parameter()
    public int activationInitiatorIdx;

    /** */
    @Parameterized.Parameters(name = "activationInitiatorIdx={0}")
    public static Iterable<Object[]> data() {
        Collection<Object[]> data = new ArrayList<>();

        for (int activationInitiatorIdx = 0; activationInitiatorIdx < TEST_NODES_CNT; activationInitiatorIdx++)
            data.add(new Object[] {activationInitiatorIdx});

        return data;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setClusterStateOnStart(INACTIVE)
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME))
            .setPluginProviders(plugin);
    }

    /** */
    @Test
    public void testErrorOnActivation() throws Exception {
        startGrids(TEST_NODES_CNT);

        plugin.markActivationBroken(true);

        try {
            grid(activationInitiatorIdx).cluster().state(ACTIVE);

            fail();
        }
        catch (IgniteException e) {
            X.hasCause(e, "expected activation exception", IgniteCheckedException.class);
        }

        Ignite cli = startClientGrid(TEST_NODES_CNT);

        assertEquals(INACTIVE, cli.cluster().state());

        plugin.markActivationBroken(false);

        grid(activationInitiatorIdx).cluster().state(ACTIVE);
        assertEquals(ACTIVE, cli.cluster().state());

        cli.cache(DEFAULT_CACHE_NAME).put(0, 0);
        assertEquals(0, cli.cache(DEFAULT_CACHE_NAME).get(0));

        cli.cluster().state(INACTIVE);
        assertEquals(INACTIVE, grid(activationInitiatorIdx).cluster().state());
        cli.cluster().state(ACTIVE);
        assertEquals(ACTIVE, grid(activationInitiatorIdx).cluster().state());
    }

    /** */
    private static final class TestPluginProvider extends AbstractTestPluginProvider implements IgniteChangeGlobalStateSupport {
        /** */
        private volatile boolean isActivationBroken;

        /** */
        public void markActivationBroken(boolean isActivationBroken) {
            this.isActivationBroken = isActivationBroken;
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return "test-plugin";
        }

        /** {@inheritDoc} */
        @Override public void onActivate(GridKernalContext ignored) throws IgniteCheckedException {
            if (isActivationBroken)
                throw new IgniteCheckedException("expected activation exception");
        }

        /** {@inheritDoc} */
        @Override public void onDeActivate(GridKernalContext ignored) {
            // No-op.
        }
    }
}

