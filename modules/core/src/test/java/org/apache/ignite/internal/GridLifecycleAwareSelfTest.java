/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorMessageInterceptor;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.client.ssl.GridSslContextFactory;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.logger.java.JavaLogger;
import org.apache.ignite.plugin.segmentation.SegmentationResolver;
import org.apache.ignite.testframework.junits.common.GridAbstractLifecycleAwareSelfTest;
import org.jetbrains.annotations.Nullable;

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
    private static class TestContextFactory extends TestLifecycleAware implements GridSslContextFactory {
        /**
         */
        TestContextFactory() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public SSLContext createSslContext() throws SSLException {
            return null;
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
    private static class TestMarshaller extends BinaryMarshaller implements LifecycleAware {
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

        clientCfg.setSslContextFactory(ctxFactory);

        lifecycleAwares.add(ctxFactory);

        TestLifecycleBean lifecycleBean = new TestLifecycleBean();

        cfg.setLifecycleBeans(lifecycleBean);

        lifecycleAwares.add(lifecycleBean);

        TestMarshaller marshaller = new TestMarshaller();

        cfg.setMarshaller(marshaller);

        lifecycleAwares.add(marshaller.lifecycleAware());

        TestLogger testLog = new TestLogger();

        cfg.setGridLogger(testLog);

        lifecycleAwares.add(testLog.lifecycleAware());

        return cfg;
    }
}