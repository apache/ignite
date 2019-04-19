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

package org.apache.ignite.ml.dataset;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.ml.dataset.feature.ObjectHistogramTest;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilderTest;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetTest;
import org.apache.ignite.ml.dataset.impl.cache.util.ComputeUtilsTest;
import org.apache.ignite.ml.dataset.impl.cache.util.DatasetAffinityFunctionWrapperTest;
import org.apache.ignite.ml.dataset.impl.cache.util.PartitionDataStorageTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilderTest;
import org.apache.ignite.ml.dataset.primitive.DatasetWrapperTest;
import org.apache.ignite.ml.dataset.primitive.SimpleDatasetTest;
import org.apache.ignite.ml.dataset.primitive.SimpleLabeledDatasetTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for all tests located in org.apache.ignite.ml.dataset.* package.
 */
@RunWith(AllTests.class)
public class DatasetTestSuite {
    /** */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite(DatasetTestSuite.class.getSimpleName());

        suite.addTest(new JUnit4TestAdapter(DatasetWrapperTest.class));
        suite.addTest(new JUnit4TestAdapter(DatasetAffinityFunctionWrapperTest.class));
        suite.addTest(new JUnit4TestAdapter(PartitionDataStorageTest.class));
        suite.addTest(new JUnit4TestAdapter(LocalDatasetBuilderTest.class));
        suite.addTest(new JUnit4TestAdapter(SimpleDatasetTest.class));
        suite.addTest(new JUnit4TestAdapter(SimpleLabeledDatasetTest.class));
        suite.addTest(new JUnit4TestAdapter(DatasetWrapperTest.class));
        suite.addTest(new JUnit4TestAdapter(ObjectHistogramTest.class));
        suite.addTest(new JUnit4TestAdapter(ComputeUtilsTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheBasedDatasetBuilderTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheBasedDatasetTest.class));

        return suite;
    }
}
