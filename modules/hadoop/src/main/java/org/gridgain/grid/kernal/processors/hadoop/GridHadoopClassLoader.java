/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import sun.misc.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * GridHadoopClassLoader
 */
public class GridHadoopClassLoader extends URLClassLoader {
    /** */
    private static volatile URL[] hadoopUrls;

    //private static volatile URL[] mainUrls;

    /**
     * Constructor.
     *
     * @throws GridException
     * @param urls
     */
    public GridHadoopClassLoader(URL[] urls) throws GridException {
        super(getHadoopUrls(), getAppClassLoader());

        if (urls != null)
            for (URL url : urls)
               addURL(url);

//        printUrls(this);
    }

    /** */
    private static ClassLoader getAppClassLoader() {
        return GridHadoopClassLoader.class.getClassLoader();
/*
        prepareUrls();

        return new URLClassLoader(mainUrls, GridHadoopClassLoader.class.getClassLoader().getParent()) {

            @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if ("junit.framework.Test".equals(name))
                    return GridHadoopClassLoader.class.getClassLoader().loadClass(name);

                try {
                    return super.loadClass(name, resolve);
                }
                catch (ClassNotFoundException e) {
                    e.printStackTrace();

                    throw e;
                }
            }
        };
*/
    }

    /** */
    private static void prepareUrls() throws GridException {
        if (hadoopUrls != null)
            return;

        synchronized (GridHadoopClassLoader.class) {
            URLClassLoader appLdr = (URLClassLoader)GridHadoopClassLoader.class.getClassLoader();

//            printUrls(appLdr);

            List<URL> hadoopUrlLst = new ArrayList<>();
//            List<URL> mainUrlsLst = new ArrayList<>();

            // Move all (exclude gridgain) maven dependencies into separate class loader
            for (URL url : appLdr.getURLs()) {
                String normUrl = url.toString().replace('\\', '/');

                hadoopUrlLst.add(url);

//                if (!normUrl.contains("m2/repository") || (
//                        normUrl.contains("/org/gridgain") ||
//                                normUrl.contains("/org/springframework") ||
//                                normUrl.contains("/repository/log4j") ||
//                                normUrl.contains("/org/slf4j") ||
//                                normUrl.contains("/repository/junit") ||
//                                normUrl.contains("/org/apache/maven"))) {
//                    mainUrlsLst.add(url);
//                }
            }


            String hadoopPrefix = getEnv("HADOOP_PREFIX", getEnv("HADOOP_HOME", null));

            if (F.isEmpty(hadoopPrefix))
                throw new GridException("Hadoop is not found");

            String commonHome = getEnv("HADOOP_COMMON_HOME", hadoopPrefix + "/share/hadoop/common");
            String hdfsHome = getEnv("HADOOP_HDFS_HOME", hadoopPrefix + "/share/hadoop/hdfs");
            String mapredHome = getEnv("HADOOP_MAPRED_HOME", hadoopPrefix + "/share/hadoop/mapreduce");

            try {
                addUrls(hadoopUrlLst, new File(commonHome + "/lib"), null);
                addUrls(hadoopUrlLst, new File(hdfsHome + "/lib"), null);
                addUrls(hadoopUrlLst, new File(mapredHome + "/lib"), null);

                addUrls(hadoopUrlLst, new File(hdfsHome), "hadoop-hdfs-");

                addUrls(hadoopUrlLst, new File(commonHome), "hadoop-common-");
                addUrls(hadoopUrlLst, new File(commonHome), "hadoop-auth-");
                addUrls(hadoopUrlLst, new File(commonHome + "/lib"), "hadoop-auth-");

                addUrls(hadoopUrlLst, new File(mapredHome), "hadoop-mapreduce-client-common");
                addUrls(hadoopUrlLst, new File(mapredHome), "hadoop-mapreduce-client-core");

            }
            catch (MalformedURLException e) {
                throw new GridException("", e);
            }

            hadoopUrls = hadoopUrlLst.toArray(new URL[hadoopUrlLst.size()]);
//            mainUrls = mainUrlsLst.toArray(new URL[mainUrlsLst.size()]);
        }
    }

    /** */
    private static void printUrls(ClassLoader appLdr) {
        if (appLdr.getParent() != null)
            printUrls(appLdr.getParent());

        System.out.println(appLdr);
        if (appLdr instanceof URLClassLoader) {
            for (URL url : ((URLClassLoader)appLdr).getURLs()) {
                System.out.println("\t" + url);
            }

        }
    }

    /** */
    private static String getEnv(String name, String def) {
        String res = System.getenv(name);

        if (F.isEmpty(res))
            return def;

        return res;
    }

    /** */
    private static URL[] getHadoopUrls() throws GridException {
        prepareUrls();

        return hadoopUrls;
    }

    /** */
    private static void addUrls(Collection<URL> res, File dir, final String startsWith) throws MalformedURLException {
        File[] files = dir.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return startsWith == null || name.startsWith(startsWith);
            }
        });

        for (File file : files)
            res.add(file.toURI().toURL());
    }

    /** */
    public Class<?> loadClassExplicitly(String name) throws ClassNotFoundException {
        //System.out.println("[LOAD CLASS] " + name);

        loadDependencies(name);

        synchronized (getClassLoadingLock(name)) {
            // First, check if the class has already been loaded
            Class c = findLoadedClass(name);

            if (c == null) {
                long t1 = System.nanoTime();
                c = findClass(name);

                // this is the defining class loader; record the stats
                PerfCounter.getFindClassTime().addElapsedTimeFrom(t1);
                PerfCounter.getFindClasses().increment();
            }

            return c;
        }
    }

    private void loadDefault() throws ClassNotFoundException {
        loadClassExplicitly("org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem");
        loadClassExplicitly("org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem");

        loadClassExplicitly("org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem$1");
        loadClassExplicitly("org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem$2");
        loadClassExplicitly("org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem$3");
        loadClassExplicitly("org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem$4");

        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopInputStream$FetchBufferPart");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopInputStream$DoubleFetchBuffer");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopProxyInputStream");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopInputStream");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopUtils");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$FileSystemClosure");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$1");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$2");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$3");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$4");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$5");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$6");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$7");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$8");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$9");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$10");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$11");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$12");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$13");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$14");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$15");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper$Delegate");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopWrapper");

        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopOutProc$1");
        loadClassExplicitly("org.gridgain.grid.kernal.ggfs.hadoop.GridGgfsHadoopOutProc");


        loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.examples.GridHadoopWordCount2");
        loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.examples.GridHadoopWordCount2Mapper");
        loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.examples.GridHadoopWordCount2Reducer");
    }

    /** */
    private void loadDependencies(String name) throws ClassNotFoundException {
        if (name.equals("org.gridgain.grid.kernal.processors.hadoop.GridHadoopMapReduceTest")) {
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.GridHadoopAbstractWordCountTest");

            return;
        }

        if (name.equals("org.gridgain.grid.kernal.processors.hadoop.GridHadoopAbstractWordCountTest")) {
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.GridHadoopAbstractSelfTest");

            return;
        }

        if (name.equals("org.gridgain.grid.kernal.processors.hadoop.GridHadoopAbstractSelfTest")) {
            loadDefault();

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.fs.GridHadoopFileSystemsUtils");

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils");
        }

        if (name.equals("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Job")) {
            loadDefault();

            //loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Job$1");

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.fs.GridHadoopFileSystemsUtils");

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2JobResourceManager$1");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2JobResourceManager");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1Splitter");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Splitter");
        }

        if (name.equals("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2TaskContext")) {
            loadDefault();

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2TaskContext$1");
            //loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2TaskContext$2");

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Context$1");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Context");

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Counter");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Task");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2MapTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2ReduceTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2SetupTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2CleanupTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopV2Partitioner");

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v2.GridHadoopWritableSerialization");

            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1Counter");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1Task$1");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1Task");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1MapTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1ReduceTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1SetupTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1CleanupTask");
            loadClassExplicitly("org.gridgain.grid.kernal.processors.hadoop.v1.GridHadoopV1Partitioner");
        }
    }

    /** {@inheritDoc} */
    @Override protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        //System.out.println("[LOAD CLASS] " + name);

        return super.loadClass(name, resolve);
    }
}
