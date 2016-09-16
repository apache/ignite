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

import junit.framework.TestSuite;
import org.apache.ignite.igfs.HadoopIgfs20FileSystemShmemPrimarySelfTest;
import org.apache.ignite.igfs.IgfsEventsTestSuite;
import org.apache.ignite.igfs.IgniteHadoopFileSystemIpcCacheSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemEmbeddedDualAsyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemEmbeddedDualSyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemEmbeddedPrimarySelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemEmbeddedSecondarySelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemExternalDualAsyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemExternalDualSyncSelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemExternalPrimarySelfTest;
import org.apache.ignite.igfs.IgniteHadoopFileSystemShmemExternalSecondarySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest;

import static org.apache.ignite.testsuites.IgniteHadoopTestSuite.downloadHadoop;

/**
 * Test suite for Hadoop file system over Ignite cache.
 * Contains tests which works on Linux and Mac OS platform only.
 */
public class IgniteIgfsLinuxAndMacOSTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        downloadHadoop();

        ClassLoader ldr = TestSuite.class.getClassLoader();

        TestSuite suite = new TestSuite("Ignite IGFS Test Suite For Linux And Mac OS");

        suite.addTest(new TestSuite(ldr.loadClass(IgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemEmbeddedPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemEmbeddedSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemEmbeddedDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemEmbeddedDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemIpcCacheSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfs20FileSystemShmemPrimarySelfTest.class.getName())));

        suite.addTest(IgfsEventsTestSuite.suite());

        return suite;
    }
}