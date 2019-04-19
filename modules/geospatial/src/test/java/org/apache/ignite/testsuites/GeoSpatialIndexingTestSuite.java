/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.internal.processors.query.h2.H2IndexingGeoSelfTest;
import org.apache.ignite.internal.processors.query.h2.H2IndexingSegmentedGeoSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Geospatial indexing tests.
 */
@RunWith(AllTests.class)
public class GeoSpatialIndexingTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("H2 Geospatial Indexing Test Suite");

        suite.addTest(new JUnit4TestAdapter(H2IndexingGeoSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(H2IndexingSegmentedGeoSelfTest.class));

        return suite;
    }
}
