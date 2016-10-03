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
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.BinarySerializationQuerySelfTest;
import org.apache.ignite.internal.processors.cache.BinarySerializationQueryWithReflectiveSerializerSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryDuplicateIndexObjectPartitionedAtomicSelfTest;
import org.apache.ignite.internal.processors.cache.binary.distributed.dht.GridCacheBinaryDuplicateIndexObjectPartitionedTransactionalSelfTest;
import org.apache.ignite.testframework.config.GridTestProperties;

/**
 * Cache query suite with binary marshaller.
 */
public class IgniteBinaryCacheQueryTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception In case of error.
     */
    public static TestSuite suite() throws Exception {
        GridTestProperties.setProperty(GridTestProperties.MARSH_CLASS_NAME, BinaryMarshaller.class.getName());

        TestSuite suite = IgniteCacheQuerySelfTestSuite.suite();

        // Serialization.
        suite.addTestSuite(BinarySerializationQuerySelfTest.class);
        suite.addTestSuite(BinarySerializationQueryWithReflectiveSerializerSelfTest.class);
        suite.addTestSuite(IgniteCacheBinaryObjectsScanSelfTest.class);

        //Should be adjusted. Not ready to be used with BinaryMarshaller.
        //suite.addTestSuite(GridCacheBinarySwapScanQuerySelfTest.class);

        suite.addTestSuite(GridCacheBinaryDuplicateIndexObjectPartitionedAtomicSelfTest.class);
        suite.addTestSuite(GridCacheBinaryDuplicateIndexObjectPartitionedTransactionalSelfTest.class);

        //TODO: the following tests= was never tested with binary. Exclude or pass?
//        suite.addTestSuite(IgniteSqlSchemaIndexingTest.class);

        return suite;
    }
}
