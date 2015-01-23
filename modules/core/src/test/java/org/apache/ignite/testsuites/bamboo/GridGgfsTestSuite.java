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

package org.apache.ignite.testsuites.bamboo;

import junit.framework.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.processors.fs.split.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.ipc.*;

/**
 * Test suite for Hadoop file system over GridGain cache.
 * Contains platform independent tests only.
 */
public class GridGgfsTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain GGFS Test Suite For Platform Independent Tests");

//        suite.addTest(new TestSuite(GridGgfsSizeSelfTest.class)); TODO Enable after GG-9035
        suite.addTest(new TestSuite(GridGgfsAttributesSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsFileInfoSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsMetaManagerSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsDataManagerSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsProcessorSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsProcessorValidationSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsCacheSelfTest.class));

        if (U.isWindows())
            suite.addTest(new TestSuite(GridGgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest.class));

        suite.addTest(new TestSuite(GridCacheGgfsPerBlockLruEvictionPolicySelfTest.class));

        suite.addTest(new TestSuite(GridGgfsStreamsSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsModesSelfTest.class));
        suite.addTest(new TestSuite(GridIpcServerEndpointDeserializerSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsMetricsSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsPrimarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsPrimaryOffheapTieredSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsPrimaryOffheapValuesSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsModeResolverSelfTest.class));

        suite.addTestSuite(GridGgfsFragmentizerSelfTest.class);
        suite.addTestSuite(GridGgfsFragmentizerTopologySelfTest.class);
        suite.addTestSuite(GridGgfsFileMapSelfTest.class);

        suite.addTestSuite(GridGgfsByteDelimiterRecordResolverSelfTest.class);
        suite.addTestSuite(GridGgfsStringDelimiterRecordResolverSelfTest.class);
        suite.addTestSuite(GridGgfsFixedLengthRecordResolverSelfTest.class);
        suite.addTestSuite(GridGgfsNewLineDelimiterRecordResolverSelfTest.class);

        suite.addTestSuite(GridGgfsTaskSelfTest.class);

        suite.addTestSuite(GridGgfsGroupDataBlockKeyMapperHashSelfTest.class);

        return suite;
    }
}
