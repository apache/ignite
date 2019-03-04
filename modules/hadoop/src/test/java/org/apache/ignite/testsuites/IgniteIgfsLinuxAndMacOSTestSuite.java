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

import java.util.ArrayList;
import java.util.List;
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
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

import static org.apache.ignite.testsuites.IgniteHadoopTestSuite.downloadHadoop;

/**
 * Test suite for Hadoop file system over Ignite cache.
 * Contains tests which works on Linux and Mac OS platform only.
 */
@RunWith(DynamicSuite.class)
public class IgniteIgfsLinuxAndMacOSTestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static List<Class<?>> suite() throws Exception {
        downloadHadoop();

        final ClassLoader ldr = new HadoopTestClassLoader();

        List<Class<?>> suite = new ArrayList<>();

        suite.add(ldr.loadClass(IgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest.class.getName()));

        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalPrimarySelfTest.class.getName()));
        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalSecondarySelfTest.class.getName()));
        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalDualSyncSelfTest.class.getName()));
        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalDualAsyncSelfTest.class.getName()));
        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientPrimarySelfTest.class.getName()));
        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientDualAsyncSelfTest.class.getName()));
        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientDualSyncSelfTest.class.getName()));
        suite.add(ldr.loadClass(IgniteHadoopFileSystemShmemExternalToClientProxySelfTest.class.getName()));

        suite.add(ldr.loadClass(IgniteHadoopFileSystemIpcCacheSelfTest.class.getName()));

        suite.add(ldr.loadClass(HadoopIgfs20FileSystemShmemPrimarySelfTest.class.getName()));

        suite.add(ldr.loadClass(IgfsNearOnlyMultiNodeSelfTest.class.getName()));

        suite.add(IgfsEventsTestSuite.class);

        return suite;
    }
}
