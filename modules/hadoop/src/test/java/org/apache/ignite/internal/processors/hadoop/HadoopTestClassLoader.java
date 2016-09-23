package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.X;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

/**
 * Hadoop test class loader aimed to provide better isolation.
 */
public class HadoopTestClassLoader extends URLClassLoader {
    /** Parent class loader. */
    private static final URLClassLoader APP_CLS_LDR = (URLClassLoader)HadoopTestClassLoader.class.getClassLoader();

    /** All participating URLs. */
    private static final URL[] URLS;

    static {
        try {
            List<URL> res = new ArrayList<>(HadoopClasspathUtils.classpathForClassLoader());

            X.println(">>> " + HadoopTestClassLoader.class.getSimpleName() + " static paths:");

            for (URL url : res)
                X.println(">>> \t" + url.toString());

            URLS = res.toArray(new URL[res.size()]);
        }
        catch (Exception e) {
            throw new IgniteException("Failed to initialize class loader JARs.", e);
        }
    }

    /**
     * Constructor.
     */
    public HadoopTestClassLoader() {
        super(URLS, APP_CLS_LDR);
    }

    /** {@inheritDoc} */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (HadoopClassLoader.loadByCurrentClassloader(name)) {
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
                throw new IgniteException("Failed to load class by test class loader: " + name, e);
            }
        }

        return super.loadClass(name, resolve);
    }
}
