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

package org.apache.ignite.internal;

import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorMessageInterceptor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.plugin.segmentation.SegmentationResolver;
import org.apache.ignite.testframework.junits.common.GridAbstractLifecycleAwareSelfTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.sslFactory;

/**
 * Test for {@link org.apache.ignite.lifecycle.LifecycleAware} support in {@link org.apache.ignite.configuration.IgniteConfiguration}.
 */
public class GridLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /**
     */
    private static class TestConnectorMessageInterceptor extends TestLifecycleAware
        implements ConnectorMessageInterceptor {
        /**
         */
        TestConnectorMessageInterceptor() {
            super(null);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onReceive(@Nullable Object obj) {
            return null;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object onSend(Object obj) {
            return null;
        }
    }

    /**
     */
    private static class TestSegmentationResolver extends TestLifecycleAware implements SegmentationResolver {
        /**
         */
        TestSegmentationResolver() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public boolean isValidSegment() throws IgniteCheckedException {
            return true;
        }
    }

    /**
     */
    private static class TestContextFactory extends TestLifecycleBean implements Factory<SSLContext> {
        /** {@inheritDoc} */
        @Override public SSLContext create() {
            return sslFactory().create();
        }
    }

    /**
     */
    private static class TestLifecycleBean extends TestLifecycleAware implements LifecycleBean {
        /**
         */
        TestLifecycleBean() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            // No-op.
        }
    }

    /**
     */
    private static class TestLogger extends JavaLogger implements LifecycleAware {
        /** */
        private final TestLifecycleAware lifecycleAware = new TestLifecycleAware(null);

        /** {@inheritDoc} */
        @Override public void start() {
            lifecycleAware.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            lifecycleAware.stop();
        }

        /**
         * @return Lifecycle aware.
         */
        TestLifecycleAware lifecycleAware() {
            return lifecycleAware;
        }
    }

    /** {@inheritDoc} */
    @Override protected final IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TestConnectorMessageInterceptor interceptor = new TestConnectorMessageInterceptor();

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setMessageInterceptor(interceptor);

        cfg.setConnectorConfiguration(clientCfg);

        lifecycleAwares.add(interceptor);

        TestSegmentationResolver segmentationRslvr = new TestSegmentationResolver();

        cfg.setSegmentationResolvers(segmentationRslvr);

        lifecycleAwares.add(segmentationRslvr);

        TestContextFactory ctxFactory = new TestContextFactory();

        clientCfg.setSslFactory(ctxFactory);

        lifecycleAwares.add(ctxFactory);

        TestContextFactory httpSslFactory = new TestContextFactory();

        clientCfg.setHttpSslFactory(httpSslFactory);

        lifecycleAwares.add(httpSslFactory);

        TestLifecycleBean lifecycleBean = new TestLifecycleBean();

        cfg.setLifecycleBeans(lifecycleBean);

        lifecycleAwares.add(lifecycleBean);

        TestLogger testLog = new TestLogger();

        cfg.setGridLogger(testLog);

        lifecycleAwares.add(testLog.lifecycleAware());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * One SSL factory that binary REST and HTTP REST share starts and stops once.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSharedSslFactory() throws Exception {
        TestContextFactory factory = new TestContextFactory();

        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName())
            .setConnectorConfiguration(new ConnectorConfiguration().setSslFactory(factory).setHttpSslFactory(factory));

        checkFactoryLifecycle(cfg, factory);
    }

    /** An HTTP SSL factory registered as a lifecycle bean starts and stops once with the node. */
    @Test
    public void testHttpSslFactoryAsLifecycleBean() throws Exception {
        TestContextFactory factory = new TestContextFactory();

        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName())
            .setLifecycleBeans(factory)
            .setConnectorConfiguration(new ConnectorConfiguration().setHttpSslFactory(factory));

        checkFactoryLifecycle(cfg, factory);
    }

    /** A lifecycle bean shared by binary and HTTP REST receives one start and one stop callback. */
    @Test
    public void testSharedSslFactoryAsLifecycleBean() throws Exception {
        TestContextFactory factory = new TestContextFactory();

        IgniteConfiguration cfg = super.getConfiguration(getTestIgniteInstanceName())
            .setLifecycleBeans(factory)
            .setConnectorConfiguration(new ConnectorConfiguration().setSslFactory(factory).setHttpSslFactory(factory));

        checkFactoryLifecycle(cfg, factory);
    }

    /** Starts and stops the node and checks the factory's lifecycle callbacks after each operation. */
    private void checkFactoryLifecycle(IgniteConfiguration cfg, TestContextFactory factory) throws Exception {
        startGrid(cfg);

        assertEquals(1, factory.startCount());
        assertEquals(0, factory.stopCount());

        stopGrid();

        assertEquals(1, factory.startCount());
        assertEquals(1, factory.stopCount());
    }
}
