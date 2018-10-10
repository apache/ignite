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

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.GridSecurityProcessorWrapper;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityPermission;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class AbstractInintiatorContextSecurityProcessorTest extends GridCommonAbstractTest {
    /** Cache name. */
    protected static final String CACHE_NAME = "TEST_CACHE";

    /** Security error message. */
    protected static final String SEC_ERR_MSG = "Test security exception.";

    /** Values. */
    protected AtomicInteger values = new AtomicInteger(0);

    /** */
    protected IgniteEx succsessSrv;

    /** */
    protected IgniteEx succsessClnt;

    /** */
    protected IgniteEx failSrv;

    /** */
    protected IgniteEx failClnt;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(
            TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS,
            "org.apache.ignite.internal.processor.security.TestSecurityProcessor"
        );

        succsessSrv = startGrid("success_server");

        succsessClnt = startGrid(getConfiguration("success_client").setClientMode(true));

        failSrv = startGrid("fail_server");

        failClnt = startGrid(getConfiguration("fail_client").setClientMode(true));

        final Set<UUID> failUUIDs = Sets.newHashSet(
            failSrv.localNode().id(), failClnt.localNode().id()
        );

        for (Ignite ignite : G.allGrids()) {
            processor((IgniteEx)ignite).authorizeConsumer(
                (name, perm, secCtx) -> {
                    if (secCtx != null) {
                        if (CACHE_NAME.equals(name) && SecurityPermission.CACHE_PUT == perm &&
                            failUUIDs.contains(secCtx.subject().id())) {

                            log.info("Failed authorize. [name=" + name + ", perm=" + perm
                            + ", secCtx=" + secCtx + "]");

                            throw new SecurityException(SEC_ERR_MSG);
                        }
                    }
                }
            );
        }

        grid("success_server").cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        System.clearProperty(TestSecurityProcessorProvider.TEST_SECURITY_PROCESSOR_CLS);

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
     * Getting of {@link TestSecurityProcessor} for the passed ignite instanse.
     *
     * @param ignite Ignite.
     */
    protected TestSecurityProcessor processor(IgniteEx ignite) {
        if (ignite.context().security() instanceof GridSecurityProcessorWrapper) {
            GridSecurityProcessorWrapper wrp = (GridSecurityProcessorWrapper)ignite.context().security();

            return (TestSecurityProcessor) wrp.original();
        }

        return (TestSecurityProcessor)ignite.context().security();
    }

    /**
     * Assert that the passed throwable contains a cause exception with given type and message.
     *
     * @param throwable Throwable.
     */
    protected void assertCauseMessage(Throwable throwable) {
        SecurityException cause = X.cause(throwable, SecurityException.class);

        assertThat(cause, notNullValue());
        assertThat(cause.getMessage(), is(SEC_ERR_MSG));
    }
}
