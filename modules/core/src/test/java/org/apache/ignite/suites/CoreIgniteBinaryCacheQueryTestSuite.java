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

package org.apache.ignite.suites;

import org.apache.ignite.internal.metric.SystemViewSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryIndexingDisabledSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQueryInternalKeysSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheQuerySqlFieldInlineSizeSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheBinaryObjectsScanWithEventsSelfTest;
import org.apache.ignite.internal.processors.cache.query.CacheDataPageScanQueryTest;
import org.apache.ignite.internal.processors.cache.query.CacheScanQueryFailoverTest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryTransformerSelfTest;
import org.apache.ignite.internal.processors.cache.query.GridCircularQueueTest;
import org.apache.ignite.internal.processors.cache.query.IgniteCacheQueryCacheDestroySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQuerySelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryTxSelfTest;
import org.apache.ignite.internal.processors.cache.query.IndexingSpiQueryWithH2IndexingSelfTest;
import org.apache.ignite.internal.sql.SqlParserBulkLoadSelfTest;
import org.apache.ignite.internal.sql.SqlParserCreateIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserDropIndexSelfTest;
import org.apache.ignite.internal.sql.SqlParserKillQuerySelfTest;
import org.apache.ignite.internal.sql.SqlParserMultiStatementSelfTest;
import org.apache.ignite.internal.sql.SqlParserSetStreamingSelfTest;
import org.apache.ignite.internal.sql.SqlParserTransactionalKeywordsSelfTest;
import org.apache.ignite.internal.sql.SqlParserUserSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for cache queries.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    SqlParserCreateIndexSelfTest.class,
    SqlParserDropIndexSelfTest.class,
    SqlParserTransactionalKeywordsSelfTest.class,
    SqlParserBulkLoadSelfTest.class,
    SqlParserSetStreamingSelfTest.class,
    SqlParserKillQuerySelfTest.class,
    SqlParserMultiStatementSelfTest.class,

    GridCacheQueryInternalKeysSelfTest.class,
    GridCacheQueryTransformerSelfTest.class,
    CacheScanQueryFailoverTest.class,
    CacheDataPageScanQueryTest.class,
    IndexingSpiQuerySelfTest.class,
    IndexingSpiQueryTxSelfTest.class,

    GridCircularQueueTest.class,
    IndexingSpiQueryWithH2IndexingSelfTest.class,

    GridCacheQueryIndexingDisabledSelfTest.class,
    IgniteCacheQueryCacheDestroySelfTest.class,
    GridCacheQuerySqlFieldInlineSizeSelfTest.class,
    SqlParserUserSelfTest.class,
    IgniteCacheBinaryObjectsScanSelfTest.class,
    IgniteCacheBinaryObjectsScanWithEventsSelfTest.class,
    SystemViewSelfTest.class
})
public class CoreIgniteBinaryCacheQueryTestSuite {
}
