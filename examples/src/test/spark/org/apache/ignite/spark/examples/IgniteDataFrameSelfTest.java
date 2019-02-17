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
