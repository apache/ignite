package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.hadoop.impl.client.HadoopClientProtocolSelfTest;
import org.apache.ignite.internal.util.typedef.F;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Separate class loader for better class handling.
 */
// TODO: Remove or adopt.
public class IgniteHadoopTestSuite2 extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        final ClassLoader ldr = new TestClassLoader();

        TestSuite suite = new TestSuite("Ignite Hadoop MR Test Suite");

        suite.addTest(new TestSuite(ldr.loadClass(HadoopClientProtocolSelfTest.class.getName())));

        return suite;
    }

    /**
     * Class loader for tests.
     */
    private static class TestClassLoader extends URLClassLoader {
        /** Parent class loader. */
        private static final URLClassLoader APP_CLS_LDR = (URLClassLoader)TestClassLoader.class.getClassLoader();

        /** */
        private static final Collection<URL> APP_JARS = F.asList(APP_CLS_LDR.getURLs());

        /** All participating URLs. */
        private static final URL[] URLS;

        static {
            try {
                List<URL> allJars = new ArrayList<>();

                allJars.addAll(APP_JARS);

                // TODO: Remove
                //allJars.addAll(HadoopClasspathUtils.classpathForClassLoader());

                List<URL> res = new ArrayList<>();

                for (URL url : allJars) {
                    String urlStr = url.toString();

                    if (urlStr.contains("modules/hadoop/") ||
                        urlStr.contains("modules/hadoop-impl/") ||
                        urlStr.contains("org/apache/hadoop")) {
                        res.add(url);

                        System.out.println(url.toString());
                    }
                }

                URLS = res.toArray(new URL[res.size()]);
            }
            catch (Exception e) {
                throw new IgniteException("Failed to initialize classloader JARs.", e);
            }
        }

        /**
         * Constructor.
         *
         * @throws Exception If failed.
         */
        public TestClassLoader() throws Exception {
            super(URLS, APP_CLS_LDR);
        }

        /** {@inheritDoc} */
        @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            try {
                synchronized (getClassLoadingLock(name)) {
                    // First, check if the class has already been loaded
                    Class c = findLoadedClass(name);

                    if (c == null) {
                        long t1 = System.nanoTime();

                        c = findClass(name);

                        // this is the defining class loader; record the stats
                        sun.misc.PerfCounter.getFindClassTime().addElapsedTimeFrom(t1);
                        sun.misc.PerfCounter.getFindClasses().increment();
                    }

                    if (resolve)
                        resolveClass(c);

                    return c;
                }
            }
            catch (NoClassDefFoundError | ClassNotFoundException e) {
                // TODO: Remove
                //System.out.println("Not founded, delegated: " + name);
            }

            return super.loadClass(name, resolve);
        }
    }
}
