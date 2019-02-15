/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
