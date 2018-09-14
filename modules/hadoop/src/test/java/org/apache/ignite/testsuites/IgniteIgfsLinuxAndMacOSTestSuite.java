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
import org.apache.ignite.internal.processors.hadoop.HadoopTestClassLoader;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.HadoopIgfs20FileSystemShmemPrimarySelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgfsEventsTestSuite;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgfsNearOnlyMultiNodeSelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemIpcCacheSelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalDualAsyncSelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalDualSyncSelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalPrimarySelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalSecondarySelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalToClientDualAsyncSelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalToClientDualSyncSelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalToClientPrimarySelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.IgniteHadoopFileSystemShmemExternalToClientProxySelfTest;
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

        final ClassLoader ldr = new HadoopTestClassLoader();

        TestSuite suite = new TestSuite("Ignite IGFS Test Suite For Linux And Mac OS");

        suite.addTest(new TestSuite(ldr.loadClass(IgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalDualAsyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientDualAsyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientProxySelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgniteHadoopFileSystemIpcCacheSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(HadoopIgfs20FileSystemShmemPrimarySelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(IgfsNearOnlyMultiNodeSelfTest.class.getName())));

        suite.addTest(IgfsEventsTestSuite.suite());

        return suite;
    }
}
