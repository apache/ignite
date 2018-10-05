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

package org.apache.ignite.internal.processor.security;

import java.lang.reflect.Field;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.GridSecurityProcessorWrp;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Security tests for a compute task.
 */
public class ComputeTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** Test key. */
    private static final String TEST_KEY = "key";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(
            TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS,
            "org.apache.ignite.internal.processor.security.TestSecurityProcessor"
        );

        super.beforeTestsStarted();

        startGrids(2).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS);

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (Ignite i : G.allGrids())
            processor((IgniteEx)i).clear();

        grid(0).cache(CACHE_NAME).remove(TEST_KEY);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration().setPersistenceEnabled(true)
                    )
            )
            .setAuthenticationEnabled(true)
            .setCacheConfiguration(
                new CacheConfiguration<>().setName(CACHE_NAME)
            );
    }

    /** */
    public void testSecProcShouldGetLocalSecCtxWhenCallOnLocalNode() {
        IgniteEx ignite = grid(0);

        processor(ignite).authorizeConsumer(
            (name, perm, secCtx) -> {
                if (CACHE_NAME.equals(name) && SecurityPermission.CACHE_PUT == perm &&
                    ignite.context().localNodeId().equals(secCtx.subject().id()))
                    throw new SecurityException("Test security exception.");
            }
        );

        Throwable throwable = GridTestUtils.assertThrowsWithCause(
            () -> ignite.compute(ignite.cluster().forLocal())
                .broadcast(() ->
                    Ignition.localIgnite().cache(CACHE_NAME).put(TEST_KEY, "value")
                )
            , SecurityException.class
        );

        assertThat(ignite.cache(CACHE_NAME).get(TEST_KEY), nullValue());

        assertCauseMessage(throwable, SecurityException.class, "Test security exception.");
    }

    /** */
    public void testSecProcShouldGetServerNearNodeSecCtxWhenCallOnRemoteNode() {
        IgniteEx remote = grid(0);

        IgniteEx near = grid(1);

        processor(remote).authorizeConsumer(
            (name, perm, secCtx) -> {
                if (CACHE_NAME.equals(name) && SecurityPermission.CACHE_PUT == perm &&
                    near.context().localNodeId().equals(secCtx.subject().id()))
                    throw new SecurityException("Test security exception.");
            }
        );

        Throwable throwable = GridTestUtils.assertThrowsWithCause(
            () -> near.compute(near.cluster().forNode(remote.localNode()))
                .broadcast(() ->
                    Ignition.localIgnite().cache(CACHE_NAME).put(TEST_KEY, "value")
                )
            , SecurityException.class
        );

        assertThat(remote.cache(CACHE_NAME).get(TEST_KEY), nullValue());

        assertCauseMessage(throwable, SecurityException.class, "Test security exception.");
    }

    /** */
    public void testSecProcShouldGetClientNearNodeSecCtxWhenCallOnRemoteNode() throws Exception {
        IgniteEx client = startGrid(getConfiguration("client-node").setClientMode(true));

        IgniteEx remote = grid(0);

        processor(remote).authorizeConsumer(
            (name, perm, secCtx) -> {
                if (CACHE_NAME.equals(name) && SecurityPermission.CACHE_PUT == perm &&
                    client.context().localNodeId().equals(secCtx.subject().id()))
                    throw new SecurityException("Test security exception.");
            }
        );

        Throwable throwable = GridTestUtils.assertThrowsWithCause(
            () -> client.compute(client.cluster().forNode(remote.localNode()))
                .broadcast(() ->
                    Ignition.localIgnite().cache(CACHE_NAME).put("key", "value")
                )
            , SecurityException.class
        );

        assertThat(remote.cache(CACHE_NAME).get("key"), nullValue());

        assertCauseMessage(throwable, SecurityException.class, "Test security exception.");
    }

    /**
     * Assert that the passed throwable contains a cause exception with given type and message.
     *
     * @param throwable Throwable.
     * @param type Type.
     * @param msg Message.
     */
    private <T extends Throwable> void assertCauseMessage(Throwable throwable, Class<T> type, String msg) {
        T cause = X.cause(throwable, type);

        assertThat(cause, notNullValue());
        assertThat(cause.getMessage(), is(msg));
    }

    /**
     * Getting of {@link TestSecurityProcessor} for the passed ignite instanse.
     *
     * @param ignite Ignite.
     */
    private TestSecurityProcessor processor(IgniteEx ignite) {
        if (ignite.context().security() instanceof GridSecurityProcessorWrp) {
            GridSecurityProcessorWrp wrp = (GridSecurityProcessorWrp)ignite.context().security();

            try {
                Field fld = GridSecurityProcessorWrp.class.getDeclaredField("original");

                boolean accessible = fld.isAccessible();
                try {
                    fld.setAccessible(true);

                    return (TestSecurityProcessor)fld.get(wrp);
                }
                finally {
                    fld.setAccessible(accessible);
                }
            }
            catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }

        return (TestSecurityProcessor)ignite.context().security();
    }
}
