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
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryCrashDetectionSelfTest;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryNativeLoaderSelfTest;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemorySpaceSelfTest;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryUtilsSelfTest;

/**
 * Shared memory test suite.
 */
public class IgniteIpcSharedMemorySelfTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite IPC Shared Memory Test Suite.");

        suite.addTest(new TestSuite(IpcSharedMemorySpaceSelfTest.class));
        suite.addTest(new TestSuite(IpcSharedMemoryUtilsSelfTest.class));
        suite.addTest(new TestSuite(IpcSharedMemoryCrashDetectionSelfTest.class));
        suite.addTest(new TestSuite(IpcSharedMemoryNativeLoaderSelfTest.class));

        return suite;
    }
}