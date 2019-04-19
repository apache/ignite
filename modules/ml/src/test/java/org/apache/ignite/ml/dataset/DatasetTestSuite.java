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
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in org.apache.ignite.ml.dataset.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    DatasetWrapperTest.class,
    DatasetAffinityFunctionWrapperTest.class,
    PartitionDataStorageTest.class,
    LocalDatasetBuilderTest.class,
    SimpleDatasetTest.class,
    SimpleLabeledDatasetTest.class,
    DatasetWrapperTest.class,
    ObjectHistogramTest.class,
    ComputeUtilsTest.class,
    CacheBasedDatasetBuilderTest.class,
    CacheBasedDatasetTest.class
})
public class DatasetTestSuite {
}
