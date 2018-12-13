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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.compress.CompressionProcessorTest;
import org.apache.ignite.internal.processors.compress.DiskPageCompressionIntegrationAsyncTest;
import org.apache.ignite.internal.processors.compress.DiskPageCompressionIntegrationTest;
import org.apache.ignite.internal.processors.compress.FileSystemUtilsTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DATA_STORAGE_PAGE_SIZE;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_DEFAULT_DISK_PAGE_COMPRESSION;
import static org.apache.ignite.configuration.DiskPageCompression.ZSTD;

/**
 */
public class IgnitePdsCompressionTestSuite {
    /**
     * @return Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite Persistent Store Test Suite (with page compression).");

        suite.addTest(new JUnit4TestAdapter(CompressionProcessorTest.class));
        suite.addTest(new JUnit4TestAdapter(FileSystemUtilsTest.class));
        suite.addTest(new JUnit4TestAdapter(DiskPageCompressionIntegrationTest.class));
        suite.addTest(new JUnit4TestAdapter(DiskPageCompressionIntegrationAsyncTest.class));

        enableCompressionByDefault();
        IgnitePdsTestSuite.addRealPageStoreTests(suite, null);

        return suite;
    }

    /**
     */
    static void enableCompressionByDefault() {
        System.setProperty(IGNITE_DEFAULT_DISK_PAGE_COMPRESSION, ZSTD.name());
        System.setProperty(IGNITE_DEFAULT_DATA_STORAGE_PAGE_SIZE, String.valueOf(8 * 1024));
    }
}
