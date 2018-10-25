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
import org.apache.ignite.igfs.IgfsFragmentizerSelfTest;
import org.apache.ignite.igfs.IgfsFragmentizerTopologySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsAtomicPrimaryMultiNodeSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsAtomicPrimarySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsAttributesSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsBackupsDualAsyncSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsBackupsDualSyncSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsBackupsPrimarySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsBlockMessageSystemPoolStarvationSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsCachePerBlockLruEvictionPolicySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsCacheSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsDualAsyncClientSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsDualSyncClientSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsLocalSecondaryFileSystemProxyClientSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsPrimaryClientSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsDataManagerSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsDualAsyncSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsDualSyncSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsFileInfoSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsFileMapSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsGroupDataBlockKeyMapperHashSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsMetaManagerSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsMetricsSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsModeResolverSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsModesSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsPrimaryMultiNodeSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsOneClientNodeTest;
import org.apache.ignite.internal.processors.igfs.IgfsPrimaryRelaxedConsistencyClientSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsPrimaryRelaxedConsistencyMultiNodeSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsPrimaryRelaxedConsistencySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsPrimarySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsProcessorValidationSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsProxySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsLocalSecondaryFileSystemProxySelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsSecondaryFileSystemInjectionSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsStartCacheTest;
import org.apache.ignite.internal.processors.igfs.IgfsStreamsSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsTaskSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsLocalSecondaryFileSystemDualAsyncClientSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsLocalSecondaryFileSystemDualAsyncSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsLocalSecondaryFileSystemDualSyncClientSelfTest;
import org.apache.ignite.internal.processors.igfs.IgfsLocalSecondaryFileSystemDualSyncSelfTest;
import org.apache.ignite.internal.processors.igfs.split.IgfsByteDelimiterRecordResolverSelfTest;
import org.apache.ignite.internal.processors.igfs.split.IgfsFixedLengthRecordResolverSelfTest;
import org.apache.ignite.internal.processors.igfs.split.IgfsNewLineDelimiterRecordResolverSelfTest;
import org.apache.ignite.internal.processors.igfs.split.IgfsStringDelimiterRecordResolverSelfTest;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Test suite for Hadoop file system over Ignite cache.
 * Contains platform independent tests only.
 */
public class IgniteIgfsTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite FS Test Suite For Platform Independent Tests");

        suite.addTest(new TestSuite(IgfsPrimarySelfTest.class));
        suite.addTest(new TestSuite(IgfsPrimaryMultiNodeSelfTest.class));

        suite.addTest(new TestSuite(IgfsPrimaryRelaxedConsistencySelfTest.class));
        suite.addTest(new TestSuite(IgfsPrimaryRelaxedConsistencyMultiNodeSelfTest.class));

        suite.addTest(new TestSuite(IgfsDualSyncSelfTest.class));
        suite.addTest(new TestSuite(IgfsDualAsyncSelfTest.class));

        suite.addTest(new TestSuite(IgfsLocalSecondaryFileSystemDualSyncSelfTest.class));
        suite.addTest(new TestSuite(IgfsLocalSecondaryFileSystemDualAsyncSelfTest.class));
        suite.addTest(new TestSuite(IgfsLocalSecondaryFileSystemDualSyncClientSelfTest.class));
        suite.addTest(new TestSuite(IgfsLocalSecondaryFileSystemDualAsyncClientSelfTest.class));

        //suite.addTest(new TestSuite(IgfsSizeSelfTest.class));
        suite.addTest(new TestSuite(IgfsAttributesSelfTest.class));
        suite.addTest(new TestSuite(IgfsFileInfoSelfTest.class));
        suite.addTest(new TestSuite(IgfsMetaManagerSelfTest.class));
        suite.addTest(new TestSuite(IgfsDataManagerSelfTest.class));
        suite.addTest(new TestSuite(IgfsProcessorSelfTest.class));
        suite.addTest(new TestSuite(IgfsProcessorValidationSelfTest.class));
        suite.addTest(new TestSuite(IgfsCacheSelfTest.class));

        if (U.isWindows())
            suite.addTest(new TestSuite(IgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest.class));

        suite.addTest(new TestSuite(IgfsCachePerBlockLruEvictionPolicySelfTest.class));

        suite.addTest(new TestSuite(IgfsStreamsSelfTest.class));
        suite.addTest(new TestSuite(IgfsModesSelfTest.class));
        suite.addTest(new TestSuite(IgfsMetricsSelfTest.class));

        suite.addTest(new TestSuite(IgfsPrimaryClientSelfTest.class));
        suite.addTest(new TestSuite(IgfsPrimaryRelaxedConsistencyClientSelfTest.class));
        suite.addTest(new TestSuite(IgfsDualSyncClientSelfTest.class));
        suite.addTest(new TestSuite(IgfsDualAsyncClientSelfTest.class));

        suite.addTest(new TestSuite(IgfsOneClientNodeTest.class));

        suite.addTest(new TestSuite(IgfsModeResolverSelfTest.class));

        //suite.addTestSuite(IgfsPathSelfTest.class);
        suite.addTestSuite(IgfsFragmentizerSelfTest.class);
        suite.addTestSuite(IgfsFragmentizerTopologySelfTest.class);
        suite.addTestSuite(IgfsFileMapSelfTest.class);

        suite.addTestSuite(IgfsByteDelimiterRecordResolverSelfTest.class);
        suite.addTestSuite(IgfsStringDelimiterRecordResolverSelfTest.class);
        suite.addTestSuite(IgfsFixedLengthRecordResolverSelfTest.class);
        suite.addTestSuite(IgfsNewLineDelimiterRecordResolverSelfTest.class);

        suite.addTestSuite(IgfsTaskSelfTest.class);

        suite.addTestSuite(IgfsGroupDataBlockKeyMapperHashSelfTest.class);

        suite.addTestSuite(IgfsStartCacheTest.class);

        suite.addTestSuite(IgfsBackupsPrimarySelfTest.class);
        suite.addTestSuite(IgfsBackupsDualSyncSelfTest.class);
        suite.addTestSuite(IgfsBackupsDualAsyncSelfTest.class);

        suite.addTestSuite(IgfsBlockMessageSystemPoolStarvationSelfTest.class);

        // TODO: Enable when IGFS failover is fixed.
        //suite.addTestSuite(IgfsBackupFailoverSelfTest.class);

        suite.addTestSuite(IgfsProxySelfTest.class);
        suite.addTestSuite(IgfsLocalSecondaryFileSystemProxySelfTest.class);
        suite.addTestSuite(IgfsLocalSecondaryFileSystemProxyClientSelfTest.class);

        suite.addTestSuite(IgfsAtomicPrimarySelfTest.class);
        suite.addTestSuite(IgfsAtomicPrimaryMultiNodeSelfTest.class);

        suite.addTestSuite(IgfsSecondaryFileSystemInjectionSelfTest.class);

        return suite;
    }
}
