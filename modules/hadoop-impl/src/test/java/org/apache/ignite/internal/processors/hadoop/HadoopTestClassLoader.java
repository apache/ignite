package org.apache.ignite.internal.processors.hadoop;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Hadoop test class loader aimed to provide better isolation.
 */
public class HadoopTestClassLoader extends URLClassLoader {
    /** Parent class loader. */
    private static final URLClassLoader APP_CLS_LDR = (URLClassLoader)HadoopTestClassLoader.class.getClassLoader();

    /** */
    private static final Collection<URL> APP_JARS = F.asList(APP_CLS_LDR.getURLs());

    /** All participating URLs. */
    private static final URL[] URLS;

    /** Singleton instance. */
    private static final HadoopTestClassLoader instance = new HadoopTestClassLoader();

    static {
        try {
            List<URL> res = new ArrayList<>();

            for (URL url : APP_JARS) {
                String urlStr = url.toString();

                if (urlStr.contains("modules/hadoop-impl/"))
                    res.add(url);
            }

            res.addAll(HadoopClasspathUtils.classpathForClassLoader());

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
     * @return Singleton instance.
     */
    public static HadoopTestClassLoader instance() {
        return instance;
    }

    /**
     * Constructor.
     */
    public HadoopTestClassLoader() {
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
            // No-op. Will delegate to parent.
        }

        return super.loadClass(name, resolve);
    }
}
