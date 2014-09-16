/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.apache.commons.compress.archivers.tar.*;
import org.apache.commons.compress.compressors.gzip.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.collections.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.streams.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.*;

/**
 * Test suite for Hadoop Map Reduce engine.
 */
public class GridHadoopTestSuite extends TestSuite {
    /** */
    private static Class<?> loadClass(Class<?> cls) throws ClassNotFoundException, GridException {
        GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);

        return ldr.loadClass(cls.getName());
    }

    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        downloadHadoop();

        TestSuite suite = new TestSuite("Gridgain Hadoop MR Test Suite");

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackExternalPrimarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackExternalSecondarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackExternalDualSyncSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackExternalDualAsyncSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedPrimarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedSecondarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedDualAsyncSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemSecondaryModeSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemClientSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoggerStateSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemLoggerSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemHandshakeSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoop20FileSystemLoopbackPrimarySelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopDualSyncSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopDualAsyncSelfTest.class)));

        suite.addTest(GridGgfsEventsTestSuite.suiteNoarchOnly());

        suite.addTest(new TestSuite(loadClass(GridHadoopFileSystemsTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopValidationSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopDefaultMapReducePlannerSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopJobTrackerSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopHashMapSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopDataStreamSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopConcurrentHashMultimapSelftest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopSkipListSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopTaskExecutionSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopV2JobSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopSerializationWrapperSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopSplitWrapperSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopTasksV1Test.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopTasksV2Test.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopMapReduceTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopMapReduceEmbeddedSelfTest.class)));

        //TODO: GG-8936 Fix and uncomment ExternalExecution tests
        //suite.addTest(new TestSuite(loadClass(GridHadoopExternalTaskExecutionSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopExternalCommunicationSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopSortingTest.class)));
        suite.addTest(new TestSuite(loadClass(GridHadoopSortingExternalTest.class)));

        suite.addTest(new TestSuite(loadClass(GridHadoopGroupingTest.class)));

        return suite;
    }

    /**
     * @throws Exception If failed.
     */
    public static void downloadHadoop() throws Exception {
        String hadoopHome = X.getSystemOrEnv("HADOOP_HOME");

        if (!F.isEmpty(hadoopHome) && new File(hadoopHome).isDirectory()) {
            X.println("HADOOP_HOME is set to: " + hadoopHome);

            return;
        }

        String ver = X.getSystemOrEnv("hadoop.version", "2.4.1");

        X.println("Will use Hadoop version: " + ver);

        String path = "hadoop-" + ver + "/hadoop-" + ver + ".tar.gz";

        List<String> urls = F.asList(
            "http://apache-mirror.rbc.ru/pub/apache/hadoop/common/",
            "http://www.eu.apache.org/dist/hadoop/common/",
            "http://www.us.apache.org/dist/hadoop/common/");

        String tmpPath = System.getProperty("java.io.tmpdir");

        X.println("tmp: " + tmpPath);

        File install = new File(tmpPath + File.separatorChar + "__hadoop");

        File home = new File(install, "hadoop-" + ver);

        X.println("Setting HADOOP_HOME to " + home.getAbsolutePath());

        System.setProperty("HADOOP_HOME", home.getAbsolutePath());

        if (home.exists()) {
            X.println("Destination directory already exists.");

            return;
        }

        for (String url : urls) {
            if (!install.mkdirs())
                throw new IOException("Failed to create directory: " + install.getAbsolutePath());

            URL u = new URL(url + path);

            X.println("Attempting to download from: " + u);

            try {
                URLConnection c = u.openConnection();

                c.connect();

                try (TarArchiveInputStream in = new TarArchiveInputStream(new GzipCompressorInputStream(
                    new BufferedInputStream(c.getInputStream(), 32 * 1024)))) {

                    TarArchiveEntry entry;

                    while ((entry = in.getNextTarEntry()) != null) {
                        File dest = new File(install, entry.getName());

                        if (entry.isDirectory()) {
                            if (!dest.mkdirs())
                                throw new IllegalStateException();
                        }
                        else {
                            X.print(" [" + dest);

                            try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dest, false),
                                    128 * 1024)) {
                                U.copy(in, out);

                                out.flush();
                            }

                            X.println("]");
                        }
                    }
                }

                return;
            }
            catch (Exception e) {
                e.printStackTrace();

                U.delete(install);
            }
        }

        throw new IllegalStateException("Failed to install Hadoop.");
    }
}
