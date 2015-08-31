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

package org.apache.ignite.session;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.checkpoint.cache.CacheCheckpointSpi;
import org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi;
import org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.hsqldb.jdbc.jdbcDataSource;

/**
 * Grid session checkpoint self test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionCheckpointSelfTest extends GridSessionCheckpointAbstractSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testSharedFsCheckpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setCheckpointSpi(spi = new SharedFsCheckpointSpi());

        checkCheckpoints(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJdbcCheckpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        jdbcDataSource ds = new jdbcDataSource();

        ds.setDatabase("jdbc:hsqldb:mem:gg_test");
        ds.setUser("sa");
        ds.setPassword("");

        JdbcCheckpointSpi spi = new JdbcCheckpointSpi();

        spi.setDataSource(ds);
        spi.setCheckpointTableName("checkpoints");
        spi.setKeyFieldName("key");
        spi.setValueFieldName("value");
        spi.setValueFieldType("longvarbinary");
        spi.setExpireDateFieldName("create_date");

        GridSessionCheckpointSelfTest.spi = spi;

        cfg.setCheckpointSpi(spi);

        checkCheckpoints(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheCheckpoint() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        String cacheName = "test-checkpoints";

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        CacheCheckpointSpi spi = new CacheCheckpointSpi();

        spi.setCacheName(cacheName);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setCheckpointSpi(spi);

        GridSessionCheckpointSelfTest.spi = spi;

        checkCheckpoints(cfg);
    }
}