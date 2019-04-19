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

import org.apache.ignite.examples.spark.JavaIgniteCatalogExample;
import org.apache.ignite.examples.spark.JavaIgniteDataFrameExample;
import org.apache.ignite.examples.spark.JavaIgniteDataFrameWriteExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 */
public class JavaIgniteDataFrameSelfTest extends GridAbstractExamplesTest {
    static final String[] EMPTY_ARGS = new String[0];

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCatalogExample() throws Exception {
        JavaIgniteCatalogExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataFrameExample() throws Exception {
        JavaIgniteDataFrameExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDataFrameWriteExample() throws Exception {
        JavaIgniteDataFrameWriteExample.main(EMPTY_ARGS);
    }
}
