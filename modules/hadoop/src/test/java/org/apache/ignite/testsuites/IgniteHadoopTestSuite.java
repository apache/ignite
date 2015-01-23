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
import org.apache.ignite.fs.*;
import org.apache.ignite.internal.processors.hadoop.*;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.*;
import org.apache.ignite.internal.processors.hadoop.shuffle.streams.*;
import org.apache.ignite.internal.processors.hadoop.taskexecutor.external.communication.*;
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

        GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);

        TestSuite suite = new TestSuite("Ignite Hadoop MR Test Suite");

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackExternalPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackExternalSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackExternalDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackExternalDualAsyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoopbackEmbeddedDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemSecondaryModeSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemClientSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoggerStateSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemLoggerSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemHandshakeSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoop20FileSystemLoopbackPrimarySelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopDualAsyncSelfTest.class.getName())));

        suite.addTest(IgniteFsEventsTestSuite.suiteNoarchOnly());

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopFileSystemsTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopValidationSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopDefaultMapReducePlannerSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopJobTrackerSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopHashMapSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopDataStreamSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopConcurrentHashMultimapSelftest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopSkipListSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopTaskExecutionSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopV2JobSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopSerializationWrapperSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopSplitWrapperSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopTasksV1Test.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopTasksV2Test.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopMapReduceTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopMapReduceEmbeddedSelfTest.class.getName())));

        //TODO: GG-8936 Fix and uncomment ExternalExecution tests
        //suite.addTest(new TestSuite(ldr.loadClass(GridHadoopExternalTaskExecutionSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopExternalCommunicationSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopSortingTest.class.getName())));

        //TODO: GG-8936 Fix and uncomment ExternalExecution tests
        //suite.addTest(new TestSuite(ldr.loadClass(GridHadoopSortingExternalTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopGroupingTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopClientProtocolSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopClientProtocolEmbeddedSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridHadoopCommandLineTest.class.getName())));

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
