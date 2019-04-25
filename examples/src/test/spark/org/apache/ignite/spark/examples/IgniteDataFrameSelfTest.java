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

package org.apache.ignite.spark.examples;

import org.apache.ignite.examples.spark.IgniteCatalogExample;
import org.apache.ignite.examples.spark.IgniteDataFrameExample;
import org.apache.ignite.examples.spark.IgniteDataFrameWriteExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 */
public class IgniteDataFrameSelfTest extends GridAbstractExamplesTest {
    static final String[] EMPTY_ARGS = new String[0];

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCatalogExample() throws Exception {
        IgniteCatalogExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataFrameExample() throws Exception {
        IgniteDataFrameExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataFrameWriteExample() throws Exception {
        IgniteDataFrameWriteExample.main(EMPTY_ARGS);
    }
}
