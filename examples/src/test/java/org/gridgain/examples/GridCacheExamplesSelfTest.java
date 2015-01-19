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

package org.gridgain.examples;

import org.gridgain.examples.datagrid.starschema.*;
import org.gridgain.examples.datagrid.store.*;
import org.gridgain.examples.datagrid.*;
import org.gridgain.examples.datagrid.datastructures.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Cache examples self test.
 */
public class GridCacheExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testGridCacheAffinityExample() throws Exception {
        CacheAffinityExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheAtomicLongExample() throws Exception {
        CacheAtomicLongExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheAtomicReferenceExample() throws Exception {
        CacheAtomicReferenceExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheAtomicSequenceExample() throws Exception {
        CacheAtomicSequenceExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheAtomicStampedExample() throws Exception {
        CacheAtomicStampedExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheCountDownLatchExample() throws Exception {
        CacheCountDownLatchExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheQueueExample() throws Exception {
        CacheQueueExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheSetExample() throws Exception {
        CacheSetExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheStoreExample() throws Exception {
        CacheStoreExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheQueryExample() throws Exception {
        CacheQueryExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheApiExample() throws Exception {
        CacheApiExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheTransactionExample() throws Exception {
        CacheTransactionExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheDataLoaderExample() throws Exception {
        CacheDataLoaderExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridPopularNumbersRealTimeExample() throws Exception {
        CachePopularNumbersExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheStoreLoaderExample() throws Exception {
        CacheStoreLoadDataExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCachePutGetExample() throws Exception {
        CachePutGetExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridSnowflakeSchemaExample() throws Exception {
        CacheStarSchemaExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGridCacheContinuousQueryExample() throws Exception {
        CacheContinuousQueryExample.main(EMPTY_ARGS);
    }
}
