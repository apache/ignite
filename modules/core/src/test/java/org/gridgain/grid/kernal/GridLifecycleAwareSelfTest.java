/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.client.ssl.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.java.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.segmentation.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import javax.net.ssl.*;

/**
 * Test for {@link GridLifecycleAware} support in {@link GridConfiguration}.
 */
public class GridLifecycleAwareSelfTest extends GridAbstractLifecycleAwareSelfTest {
    /**
     */
    private static class TestClientMessageInterceptor extends TestLifecycleAware
        implements GridClientMessageInterceptor {
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
    private static class TestLifecycleBean extends TestLifecycleAware implements GridLifecycleBean {
        /**
         */
        TestLifecycleBean() {
            super(null);
        }

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(GridLifecycleEventType evt) throws GridException {
            // No-op.
        }
    }

    /**
     */
    private static class TestMarshaller extends GridOptimizedMarshaller implements GridLifecycleAware {
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
    private static class TestLogger extends GridJavaLogger implements GridLifecycleAware {
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
    @Override protected final GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        TestClientMessageInterceptor interceptor = new TestClientMessageInterceptor();

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

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
