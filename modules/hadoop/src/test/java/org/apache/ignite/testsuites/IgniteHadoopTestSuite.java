/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import junit.framework.TestSuite;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.client.hadoop.HadoopClientProtocolEmbeddedSelfTest;
import org.apache.ignite.client.hadoop.HadoopClientProtocolSelfTest;
import org.apache.ignite.igfs.Hadoop1OverIgfsDualAsyncTest;
import org.apache.ignite.igfs.Hadoop1OverIgfsDualSyncTest;
import org.apache.ignite.igfs.HadoopFIleSystemFactorySelfTest;
import org.apache.ignite.igfs.HadoopIgfs20FileSystemLoopbackPrimarySelfTest;
import org.apache.ignite.igfs.HadoopIgfsDualAsyncSelfTest;
import org.apache.ignite.igfs.HadoopIgfsDualSyncSelfTest;
import org.apache.ignite.igfs.HadoopSecondaryFileSystemConfigurationTest;
import org.apache.ignite.igfs.IgfsEventsTestSuite;
import org.apache.ignite.igfs.IgniteHadoopFileSystemClientSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemHandshakeSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoggerSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoggerStateSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackEmbeddedDualAsyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackEmbeddedPrimarySelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackEmbeddedSecondarySelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackExternalDualAsyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackExternalDualSyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackExternalPrimarySelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemLoopbackExternalSecondarySelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemSecondaryFileSystemInitializationSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemSecondaryModeSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopClassLoaderTest;
import org.apache.ignite.internal.processors.hadoop.HadoopCommandLineTest;
import org.apache.ignite.internal.processors.hadoop.HadoopDefaultMapReducePlannerSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopFileSystemsTest;
import org.apache.ignite.internal.processors.hadoop.HadoopGroupingTest;
import org.apache.ignite.internal.processors.hadoop.HadoopJobTrackerSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReduceEmbeddedSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopMapReduceTest;
import org.apache.ignite.internal.processors.hadoop.HadoopSerializationWrapperSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopSnappyTest;
import org.apache.ignite.internal.processors.hadoop.HadoopSortingTest;
import org.apache.ignite.internal.processors.hadoop.HadoopSplitWrapperSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskExecutionSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopTasksV1Test;
import org.apache.ignite.internal.processors.hadoop.HadoopTasksV2Test;
import org.apache.ignite.internal.processors.hadoop.HadoopV2JobSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopValidationSelfTest;
import org.apache.ignite.internal.processors.hadoop.HadoopSnappyFullMapReduceTest;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopConcurrentHashMultimapSelftest;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopHashMapSelfTest;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopSkipListSelfTest;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.HadoopDataStreamSelfTest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.testframework.GridTestUtils.modeToPermissionSet;

/**
 * Test suite for Hadoop Map Reduce engine.
 */
public class IgniteHadoopTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        downloadHadoop();
        downloadHive();

        final ClassLoader ldr = TestSuite.class.getClassLoader();

        TestSuite suite = new TestSuite("Ignite Hadoop MR Test Suite");

        suite.addTest(new TestSuite(ldr.loadClass(HadoopSnappyTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopSnappyFullMapReduceTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopClassLoaderTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfs20FileSystemLoopbackPrimarySelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfsDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfsDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(Hadoop1OverIgfsDualSyncTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(Hadoop1OverIgfsDualAsyncTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopFIleSystemFactorySelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalDualAsyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemSecondaryModeSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemSecondaryFileSystemInitializationSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemClientSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoggerStateSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoggerSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemHandshakeSelfTest.class.getName())));

        suite.addTest(IgfsEventsTestSuite.suiteNoarchOnly());

        suite.addTest(new TestSuite(ldr.loadClass(HadoopFileSystemsTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopValidationSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopDefaultMapReducePlannerSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopJobTrackerSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopHashMapSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopDataStreamSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopConcurrentHashMultimapSelftest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopSkipListSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopTaskExecutionSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopV2JobSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopSerializationWrapperSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopSplitWrapperSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopTasksV1Test.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopTasksV2Test.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopMapReduceTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopMapReduceEmbeddedSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopSortingTest.class.getName())));

//        suite.addTest(new TestSuite(ldr.loadClass(HadoopExternalTaskExecutionSelfTest.class.getName())));
//        suite.addTest(new TestSuite(ldr.loadClass(HadoopExternalCommunicationSelfTest.class.getName())));
//        suite.addTest(new TestSuite(ldr.loadClass(HadoopSortingExternalTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopGroupingTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopClientProtocolSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopClientProtocolEmbeddedSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopCommandLineTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopSecondaryFileSystemConfigurationTest.class.getName())));
        return suite;
    }

    /**
     * @throws Exception If failed.
     */
    public static void downloadHive() throws Exception {
        String ver = IgniteSystemProperties.getString("hive.version", "1.2.1");

        X.println("Will use Hive version: " + ver);

        String downloadPath = "hive/hive-" + ver + "/apache-hive-" + ver + "-bin.tar.gz";

        download("Hive", "HIVE_HOME", downloadPath, "apache-hive-" + ver + "-bin");
    }

    /**
     * @throws Exception If failed.
     */
    public static void downloadHadoop() throws Exception {
        String ver = IgniteSystemProperties.getString("hadoop.version", "2.4.1");

        X.println("Will use Hadoop version: " + ver);

        String downloadPath = "hadoop/core/hadoop-" + ver + "/hadoop-" + ver + ".tar.gz";

        download("Hadoop", "HADOOP_HOME", downloadPath, "hadoop-" + ver);
    }

    /**
     *  Downloads and extracts an Apache product.
     *
     * @param appName Name of application for log messages.
     * @param homeVariable Pointer to home directory of the component.
     * @param downloadPath Relative download path of tar package.
     * @param destName Local directory name to install component.
     * @throws Exception If failed.
     */
    private static void download(String appName, String homeVariable, String downloadPath, String destName)
        throws Exception {
        String homeVal = IgniteSystemProperties.getString(homeVariable);

        if (!F.isEmpty(homeVal) && new File(homeVal).isDirectory()) {
            X.println(homeVariable + " is set to: " + homeVal);

            return;
        }

        List<String> urls = F.asList(
            "http://archive.apache.org/dist/",
            "http://apache-mirror.rbc.ru/pub/apache/",
            "http://www.eu.apache.org/dist/",
            "http://www.us.apache.org/dist/");

        String tmpPath = System.getProperty("java.io.tmpdir");

        X.println("tmp: " + tmpPath);

        File install = new File(tmpPath + File.separatorChar + "__hadoop");

        File home = new File(install, destName);

        X.println("Setting " + homeVariable + " to " + home.getAbsolutePath());

        System.setProperty(homeVariable, home.getAbsolutePath());

        File successFile = new File(home, "__success");

        if (home.exists()) {
            if (successFile.exists()) {
                X.println(appName + " distribution already exists.");

                return;
            }

            X.println(appName + " distribution is invalid and it will be deleted.");

            if (!U.delete(home))
                throw new IOException("Failed to delete directory: " + install.getAbsolutePath());
        }

        for (String url : urls) {
            if (!(install.exists() || install.mkdirs()))
                throw new IOException("Failed to create directory: " + install.getAbsolutePath());

            URL u = new URL(url + downloadPath);

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
                        else if (entry.isSymbolicLink()) {
                            // Important: in Hadoop installation there are symlinks, we need to create them:
                            Path theLinkItself = Paths.get(install.getAbsolutePath(), entry.getName());

                            Path linkTarget = Paths.get(entry.getLinkName());

                            Files.createSymbolicLink(theLinkItself, linkTarget);
                        }
                        else {
                            File parent = dest.getParentFile();

                            if (!(parent.exists() || parent.mkdirs()))
                                throw new IllegalStateException();

                            X.print(" [" + dest);

                            try (BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(dest, false),
                                    128 * 1024)) {
                                U.copy(in, out);

                                out.flush();
                            }

                            Files.setPosixFilePermissions(dest.toPath(), modeToPermissionSet(entry.getMode()));

                            X.println("]");
                        }
                    }
                }

                if (successFile.createNewFile())
                    return;
            }
            catch (Exception e) {
                e.printStackTrace();

                U.delete(install);
            }
        }

        throw new IllegalStateException("Failed to install " + appName + ".");
    }
}