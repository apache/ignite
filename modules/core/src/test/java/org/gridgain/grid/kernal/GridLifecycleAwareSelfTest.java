/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.logger.java.*;
import org.apache.ignite.marshaller.optimized.*;
import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.apache.ignite.plugin.segmentation.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;

/**
 * Test for {@link org.apache.ignite.lifecycle.LifecycleAware} support in {@link org.apache.ignite.configuration.IgniteConfiguration}.
 */
public class GridLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /**
     */
    private static class TestClientMessageInterceptor extends TestLifecycleAware
        implements ClientMessageInterceptor {
        /**
         */
        TestClientMessageInterceptor() {
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
    private static class TestSegmentationResolver extends TestLifecycleAware implements GridSegmentationResolver {
        /**
         */
        TestSegmentationResolver() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public boolean isValidSegment() throws GridException {
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
        @Override public void onLifecycleEvent(LifecycleEventType evt) throws GridException {
            // No-op.
        }
    }

    /**
     */
    private static class TestMarshaller extends IgniteOptimizedMarshaller implements LifecycleAware {
        /** */
        private final TestLifecycleAware lifecycleAware = new TestLifecycleAware(null);

        /** {@inheritDoc} */
        @Override public void start() throws GridException {
            lifecycleAware.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() throws GridException {
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
    private static class TestLogger extends IgniteJavaLogger implements LifecycleAware {
        /** */
        private final TestLifecycleAware lifecycleAware = new TestLifecycleAware(null);

        /** {@inheritDoc} */
        @Override public void start() throws GridException {
            lifecycleAware.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() throws GridException {
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
    @Override protected final IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TestClientMessageInterceptor interceptor = new TestClientMessageInterceptor();

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        clientCfg.setClientMessageInterceptor(interceptor);

        cfg.setClientConnectionConfiguration(clientCfg);

        lifecycleAwares.add(interceptor);

        TestSegmentationResolver segmentationRslvr = new TestSegmentationResolver();

        cfg.setSegmentationResolvers(segmentationRslvr);

        lifecycleAwares.add(segmentationRslvr);

        TestContextFactory ctxFactory = new TestContextFactory();

        clientCfg.setRestTcpSslContextFactory(ctxFactory);

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
