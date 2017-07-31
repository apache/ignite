package org.apache.ignite.p2p;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * Test P2P deployment task which was loaded with external classloader and was wrapped by class loaded with current
 */
public class GridP2PDetectSelfTest extends GridCommonAbstractTest {

    private static final String EXT_REMOTE_TASK_NAME = "org.apache.ignite.tests.p2p.GridP2PSimpleDeploymentTask";

    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration conf = super.getConfiguration(name);
        conf.setPeerClassLoadingEnabled(true);
        conf.setClientMode(isRemoteJvm());
        return conf;
    }

    @Override protected boolean isMultiJvm() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public void testP2PDetection() throws Exception {
        startGrid(0);
        try {
            IgniteEx client = startGrid(1);
            URL[] classpath = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};
            GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(classpath);
            Class<IgniteCallable<Integer>> extCls = (Class<IgniteCallable<Integer>>)ldr.loadClass(EXT_REMOTE_TASK_NAME);
            IgniteCallable<Integer> externalCallable = extCls.newInstance();
            IgniteCallable<Integer> currentCallable = new CurrentCallable<>(externalCallable);
            Integer result = ((IgniteProcessProxy)client).remoteCompute().call(currentCallable);
            assert result == 1;
        }
        catch (MalformedURLException | ClassNotFoundException e) {
            throw new RuntimeException("Define property p2p.uri.cls", e);
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * Wraps callable to confuse classloader detection
     *
     * @param <T> result type
     */
    private static class CurrentCallable<T> implements IgniteCallable<T> {

        private final IgniteCallable<T> parent;

        public CurrentCallable(IgniteCallable<T> parent) {
            this.parent = parent;
        }

        /** {@inheritDoc} */
        @Override public T call() throws Exception {
            return parent.call();
        }
    }

}
