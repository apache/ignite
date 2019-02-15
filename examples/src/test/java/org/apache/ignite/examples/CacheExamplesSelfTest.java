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

package org.apache.ignite.examples;

import org.apache.ignite.examples.datagrid.CacheAffinityExample;
import org.apache.ignite.examples.datagrid.CacheEntryProcessorExample;
import org.apache.ignite.examples.datagrid.CacheApiExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

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
    @Test
    public void testCacheAffinityExample() throws Exception {
        CacheAffinityExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
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
