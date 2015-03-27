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

import junit.framework.*;
import org.apache.commons.compress.archivers.tar.*;
import org.apache.commons.compress.compressors.gzip.*;
import org.apache.ignite.*;
import org.apache.ignite.client.hadoop.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.*;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;

import static org.apache.ignite.testframework.GridTestUtils.*;

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

        HadoopClassLoader ldr = new HadoopClassLoader(null, "test");

        TestSuite suite = new TestSuite("Ignite Hadoop MR Test Suite");

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackExternalDualAsyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoopbackEmbeddedDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemSecondaryModeSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemClientSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoggerStateSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemLoggerSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemHandshakeSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfs20FileSystemLoopbackPrimarySelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfsDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfsDualAsyncSelfTest.class.getName())));

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

        // TODO: IGNITE-404: Uncomment when fixed.
        //suite.addTest(new TestSuite(ldr.loadClass(HadoopExternalTaskExecutionSelfTest.class.getName())));
        //suite.addTest(new TestSuite(ldr.loadClass(HadoopExternalCommunicationSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopSortingExternalTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopGroupingTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopClientProtocolSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(HadoopClientProtocolEmbeddedSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopCommandLineTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopSecondaryFileSystemConfigurationTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(Hadoop1OverIgfsDualSyncTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(Hadoop1OverIgfsDualAsyncTest.class.getName())));
        return suite;
    }

    /**
     * @throws Exception If failed.
     */
    public static void downloadHive() throws Exception {
        String ver = IgniteSystemProperties.getString("hive.version", "0.13.1");

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

        String downloadPath = "hadoop/common/hadoop-" + ver + "/hadoop-" + ver + ".tar.gz";

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
