package org.apache.ignite.internal.processors.hadoop;

import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.internal.processors.hadoop.SnappyUtil.addHadoopNativeLibToJavaLibraryPath;

/**
 * Tests Hadoop Snappy codec invocation.
 */
public class HadoopSnappyTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        addHadoopNativeLibToJavaLibraryPath();
    }

    /**
     * Checks Snappy Codec invokeing same simple test from .
     *
     * @throws Exception On error.
     */
    public void testSnappyCodec() throws Exception {
        // Run Snappy test in default class loader:
        SnappyUtil.printDiagnosticAndTestSnappy(getClass(), null);

        // Run the same in several more class loaders simulating jobs and tasks:
        for (int i=0; i<5; i++) {
            ClassLoader cl = new HadoopClassLoader(null, "cl-" + i);

            Class<?> clazz = (Class)Class.forName(SnappyUtil.class.getName(), true, cl);

            Method m = clazz.getDeclaredMethod("printDiagnosticAndTestSnappy", Class.class,
                cl.loadClass(Configuration.class.getName()));

            m.setAccessible(true);

            m.invoke(null, clazz, null);
        }
    }
}
