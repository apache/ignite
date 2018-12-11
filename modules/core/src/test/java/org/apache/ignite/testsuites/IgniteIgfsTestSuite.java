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
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite FS Test Suite For Platform Independent Tests");

        suite.addTest(new JUnit4TestAdapter(IgfsPrimarySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsPrimaryMultiNodeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsPrimaryRelaxedConsistencySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsPrimaryRelaxedConsistencyMultiNodeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsDualSyncSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsDualAsyncSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsLocalSecondaryFileSystemDualSyncSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsLocalSecondaryFileSystemDualAsyncSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsLocalSecondaryFileSystemDualSyncClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsLocalSecondaryFileSystemDualAsyncClientSelfTest.class));

        //suite.addTest(new JUnit4TestAdapter(IgfsSizeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsAttributesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsFileInfoSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsMetaManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsDataManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsProcessorSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsProcessorValidationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsCacheSelfTest.class));

        if (U.isWindows())
            suite.addTest(new JUnit4TestAdapter(IgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsCachePerBlockLruEvictionPolicySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsStreamsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsModesSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsMetricsSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsPrimaryClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsPrimaryRelaxedConsistencyClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsDualSyncClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsDualAsyncClientSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsOneClientNodeTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsModeResolverSelfTest.class));

        //suite.addTest(new JUnit4TestAdapter(IgfsPathSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsFragmentizerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsFragmentizerTopologySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsFileMapSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsByteDelimiterRecordResolverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsStringDelimiterRecordResolverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsFixedLengthRecordResolverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsNewLineDelimiterRecordResolverSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsTaskSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsGroupDataBlockKeyMapperHashSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsStartCacheTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsBackupsPrimarySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsBackupsDualSyncSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsBackupsDualAsyncSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsBlockMessageSystemPoolStarvationSelfTest.class));

        // TODO: Enable when IGFS failover is fixed.
        //suite.addTest(new JUnit4TestAdapter(IgfsBackupFailoverSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsProxySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsLocalSecondaryFileSystemProxySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsLocalSecondaryFileSystemProxyClientSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsAtomicPrimarySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgfsAtomicPrimaryMultiNodeSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgfsSecondaryFileSystemInjectionSelfTest.class));

        return suite;
    }
}
