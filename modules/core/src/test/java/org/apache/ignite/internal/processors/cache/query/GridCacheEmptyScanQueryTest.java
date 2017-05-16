package org.apache.ignite.internal.processors.cache.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.test.ignite2190.Employee;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

/**
 * @author NIzhikov
 */
public class GridCacheEmptyScanQueryTest extends GridCommonAbstractTest {
    /**
     * jar name contains Employee class
     */
    private static final String IGNITE_2190_1_0_JAR = "ignite-2190-1.0.jar";

    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        cfg.setClientMode(false);
        cfg.setPeerClassLoadingEnabled(true);
        return cfg;
    }


    public void testEmptyScanQueryWithOptimizedMarshaller() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME,
            "org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller");
        runEmptyScanQuery();
    }

    public void testEmptyScanQueryWithBinaryMarshaller() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME,
            "org.apache.ignite.internal.binary.BinaryMarshaller");
        runEmptyScanQuery();
    }


    public void runEmptyScanQuery() throws Exception {
        Ignite local = startGrid(0);
        Ignite remote = startGrid(1);
        stopGrid(0);

        Ignition.setClientMode(true);
        try (Ignite client = Ignition.start(getLocalConfiguration())) {
            CacheConfiguration<Integer, Employee> cacheCfg = new CacheConfiguration<>("testCache");

            IgniteCache<Integer, Employee> cache = client.getOrCreateCache(cacheCfg);

            cache.put(1, new Employee(1, "name 1"));
            cache.put(2, new Employee(2, "name 2"));

            assertEquals("Size of result of empty ScanQuery should be 2", 2,
                cache.query(new ScanQuery<Integer, Employee>()).getAll().size());
        }
        finally {
            stopAllGrids();
        }
    }

    private IgniteConfiguration getLocalConfiguration() throws Exception {
        final IgniteConfiguration cfg = getConfiguration();
        cfg.setClientMode(true);
        return cfg;
    }

    @Override protected IgniteProcessProxy igniteProcessProxy(IgniteConfiguration cfg, Ignite locNode,
        boolean resetDiscovery)
        throws Exception {
        return new IgniteProcessProxy(cfg, log, grid(0)) {
            @Override protected Collection<String> filteredJvmArgs(IgniteConfiguration cfg) {
                Collection<String> defaultFilteredJvmArgs = super.filteredJvmArgs(cfg);
                List<String> filteredJvmArgs = new ArrayList<>();

                boolean classpathFound = false;
                Iterator<String> iDefaultFilteredJvmArgs = defaultFilteredJvmArgs.iterator();
                while (iDefaultFilteredJvmArgs.hasNext()) {
                    String arg = iDefaultFilteredJvmArgs.next();
                    if ("-cp".equals(arg) || "-classpath".equals(arg)) {
                        filteredJvmArgs.add(arg);
                        filteredJvmArgs.add(excludeIgnite2190JarFromClasspath(iDefaultFilteredJvmArgs.next()));
                        classpathFound = true;
                    }
                    else
                        filteredJvmArgs.add(arg);
                }

                if (!classpathFound) {
                    String classpath = System.getProperty("java.class.path");
                    String sfcp = System.getProperty("surefire.test.class.path");
                    if (sfcp != null)
                        classpath += System.getProperty("path.separator") + sfcp;

                    filteredJvmArgs.add("-cp");
                    filteredJvmArgs.add(excludeIgnite2190JarFromClasspath(classpath));
                }

                return filteredJvmArgs;
            }

            /**
             * Excluding ignite-2190-1.0.jar so Employee class become invisible for a remote node
             */
            String excludeIgnite2190JarFromClasspath(String classpath) {
                final String pathSeparator = System.getProperty("path.separator");
                final String[] classpathArr = classpath.split(pathSeparator);
                classpath = "";
                for (String aClasspathArr : classpathArr) {
                    if (!aClasspathArr.contains(IGNITE_2190_1_0_JAR)) {
                        classpath += (classpath.length() > 0 ? pathSeparator : "") + aClasspathArr;
                    }
                }
                return classpath;
            }
        };
    }
}
