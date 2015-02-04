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

package org.apache.ignite.examples;

import org.apache.ignite.examples.datagrid.*;
import org.apache.ignite.examples.datagrid.datastructures.*;
import org.apache.ignite.examples.datagrid.starschema.*;
import org.apache.ignite.examples.datagrid.store.*;
import org.apache.ignite.testframework.junits.common.*;

/**
 * Cache examples self test.
 */
public class CacheExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    public void testCacheAffinityExample() throws Exception {
        CacheAffinityExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheAtomicLongExample() throws Exception {
        CacheAtomicLongExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheAtomicReferenceExample() throws Exception {
        CacheAtomicReferenceExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheAtomicSequenceExample() throws Exception {
        CacheAtomicSequenceExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheAtomicStampedExample() throws Exception {
        CacheAtomicStampedExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheCountDownLatchExample() throws Exception {
        CacheCountDownLatchExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheQueueExample() throws Exception {
        CacheQueueExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheSetExample() throws Exception {
        CacheSetExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheStoreExample() throws Exception {
        CacheStoreExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheQueryExample() throws Exception {
        CacheQueryExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiExample() throws Exception {
        CacheApiExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheTransactionExample() throws Exception {
        CacheTransactionExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheDataLoaderExample() throws Exception {
        CacheDataLoaderExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPopularNumbersRealTimeExample() throws Exception {
        CachePopularNumbersExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheStoreLoaderExample() throws Exception {
        CacheStoreLoadDataExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCachePutGetExample() throws Exception {
        CachePutGetExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSnowflakeSchemaExample() throws Exception {
        CacheStarSchemaExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheContinuousQueryExample() throws Exception {
        CacheContinuousQueryExample.main(EMPTY_ARGS);
    }
}
