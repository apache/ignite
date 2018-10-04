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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.GridSecurityProcessorWrp;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Security tests for a compute task.
 */
public class ComputeTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();

        cleanPersistenceDir();
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

    /**
     * вызов опреции в лямбде для локального узла. будет передан контекст локального узла
     */
    public void testSecProcShouldGetLocalSecCtxWhenCallOnLocalNode() throws Exception {
        IgniteEx ignite = grid(0);
//        IgniteEx ignite = startGrid(getConfiguration("client-node").setClientMode(true));

        TestSecurityProcessor secProc = processor(ignite);

        AtomicBoolean call = new AtomicBoolean(false);

        secProc.authorizeConsumer(
            (name, perm, secCtx) -> {
                if (CACHE_NAME.equals(name) && SecurityPermission.CACHE_PUT == perm) {
                    assertThat(secCtx.subject().id(), is(ignite.context().localNodeId()));
                    call.set(true);
                }
                else
                    System.out.println(
                        "MY_DEBUG name=" + name + ", perm=" + perm + ", secCtx=" + secCtx
                    );
            }
        );

        ignite.compute(ignite.cluster().forLocal())
            .broadcast(() ->
                Ignition.localIgnite().cache(CACHE_NAME).put("key", "value")
            );

        assertThat(call.get(), is(true));
    }

    private TestSecurityProcessor processor(IgniteEx ignite) {
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

    /**
     * вызов опреции в лямбде для удалннного узла. будет передан контекст узла инициатора
     */
    public void testSecProcShouldGetNearNodeSecCtxWhenCallOnRemoteNode() {

    }

}
