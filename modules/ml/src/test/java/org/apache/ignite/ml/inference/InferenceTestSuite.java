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

package org.apache.ignite.ml.inference;

import org.apache.ignite.ml.inference.builder.IgniteDistributedModelBuilderTest;
import org.apache.ignite.ml.inference.builder.SingleModelBuilderTest;
import org.apache.ignite.ml.inference.builder.ThreadedModelBuilderTest;
import org.apache.ignite.ml.inference.storage.model.DefaultModelStorageTest;
import org.apache.ignite.ml.inference.util.DirectorySerializerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in {@link org.apache.ignite.ml.inference} package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    SingleModelBuilderTest.class,
    ThreadedModelBuilderTest.class,
    DirectorySerializerTest.class,
    DefaultModelStorageTest.class,
    IgniteDistributedModelBuilderTest.class,
    IgniteModelStorageUtilTest.class
})
public class InferenceTestSuite {
}
