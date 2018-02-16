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

import org.apache.ignite.examples.datagrid.CacheAffinityExample;
import org.apache.ignite.examples.datagrid.CacheEntryProcessorExample;
import org.apache.ignite.examples.datagrid.CacheApiExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;

//import org.apache.ignite.examples.datagrid.starschema.*;
//import org.apache.ignite.examples.datagrid.store.dummy.*;
//import org.apache.ignite.examples.datastructures.*;

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
    public void testCacheEntryProcessorExample() throws Exception {
        CacheEntryProcessorExample.main(EMPTY_ARGS);
    }

//    TODO: IGNITE-711 next example(s) should be implemented for java 8
//    or testing method(s) should be removed if example(s) does not applicable for java 8.
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheAtomicLongExample() throws Exception {
//        IgniteAtomicLongExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheAtomicReferenceExample() throws Exception {
//        IgniteAtomicReferenceExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheAtomicSequenceExample() throws Exception {
//        IgniteAtomicSequenceExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheAtomicStampedExample() throws Exception {
//        IgniteAtomicStampedExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheCountDownLatchExample() throws Exception {
//        IgniteCountDownLatchExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheQueueExample() throws Exception {
//        IgniteQueueExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheSetExample() throws Exception {
//        IgniteSetExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheDummyStoreExample() throws Exception {
//        CacheDummyStoreExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheQueryExample() throws Exception {
//        CacheQueryExample.main(EMPTY_ARGS);
//    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiExample() throws Exception {
        CacheApiExample.main(EMPTY_ARGS);
    }

//    TODO: IGNITE-711 next example(s) should be implemented for java 8
//    or testing method(s) should be removed if example(s) does not applicable for java 8.
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheTransactionExample() throws Exception {
//        CacheTransactionExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheDataStreamerExample() throws Exception {
//        CacheDataStreamerExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCachePutGetExample() throws Exception {
//        CachePutGetExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testSnowflakeSchemaExample() throws Exception {
//        CacheStarSchemaExample.main(EMPTY_ARGS);
//    }
//
//    /**
//     * @throws Exception If failed.
//     */
//    public void testCacheContinuousQueryExample() throws Exception {
//        CacheContinuousQueryExample.main(EMPTY_ARGS);
//    }
}
