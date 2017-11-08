package org.apache.ignite.p2p;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import java.lang.reflect.Constructor;
import java.net.URL;

/**
 */
public class DeploymentClassLoaderCallableTest extends GridCommonAbstractTest {
    /** */
    private static final String RUN_CLS = "org.apache.ignite.tests.p2p.compute.ExternalCallable";
    private static final String RUN_CLS1 = "org.apache.ignite.tests.p2p.compute.ExternalCallable1";
    private static final String RUN_CLS2 = "org.apache.ignite.tests.p2p.compute.ExternalCallable2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPeerClassLoadingEnabled(true);
    }

    public void testDeploymentFromSecondAndThird() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);
            final Ignite ignite2 = startGrid(2);
            final Ignite ignite3 = startGrid(3);

            runJob0(ignite2, 10_000);

            runJob1(ignite3, 10_000);
            runJob2(ignite3, 10_000);
        }
        finally {
            stopAllGrids();
        }
    }

    public void testDeploymentFromEach() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);
            final Ignite ignite2 = startGrid(2);
            final Ignite ignite3 = startGrid(3);

            runJob0(ignite1, 10_000);

            runJob1(ignite2, 10_000);

            runJob2(ignite3, 10_000);
        }
        finally {
            stopAllGrids();
        }
    }

    public void testDeploymentFromOne() throws Exception {
        try {
            final Ignite ignite1 = startGrid(1);
            final Ignite ignite2 = startGrid(2);
            final Ignite ignite3 = startGrid(3);

            runJob0(ignite3, 10_000);
            runJob1(ignite3, 10_000);
            runJob2(ignite3, 10_000);
        }
        finally {
            stopAllGrids();
        }
    }

    private void runJob1(Ignite ignite, long timeout) throws Exception {
        ClassLoader testClassLoader1 = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS2);

        Constructor ctor = testClassLoader1.loadClass(RUN_CLS1).getConstructor();
        ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<? extends Object>)ctor.newInstance());
    }

    @NotNull private void runJob0(Ignite ignite, long timeout) throws Exception {
        ClassLoader testClassLoader = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS1, RUN_CLS2);

        Constructor ctor = testClassLoader.loadClass(RUN_CLS).getConstructor();
        ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<? extends Object>)ctor.newInstance());
    }

    @NotNull private void runJob2(Ignite ignite, long timeout) throws Exception {
        ClassLoader testClassLoader = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))}, RUN_CLS, RUN_CLS1);

        Constructor ctor = testClassLoader.loadClass(RUN_CLS2).getConstructor();
        ignite.compute().withTimeout(timeout).broadcast((IgniteCallable<? extends Object>)ctor.newInstance());
    }
}
