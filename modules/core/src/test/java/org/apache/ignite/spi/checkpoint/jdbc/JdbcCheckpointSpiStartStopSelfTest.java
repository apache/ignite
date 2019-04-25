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

package org.apache.ignite.spi.checkpoint.jdbc;

import org.apache.ignite.spi.GridSpiStartStopAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.hsqldb.jdbc.jdbcDataSource;

/**
 * Grid jdbc checkpoint SPI start stop self test.
 */
@GridSpiTest(spi = JdbcCheckpointSpi.class, group = "Checkpoint SPI")
public class JdbcCheckpointSpiStartStopSelfTest
    extends GridSpiStartStopAbstractTest<JdbcCheckpointSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(JdbcCheckpointSpi spi) throws Exception {
        jdbcDataSource ds = new jdbcDataSource();

        ds.setDatabase("jdbc:hsqldb:mem:gg_test_" + getClass().getSimpleName());
        ds.setUser("sa");
        ds.setPassword("");

        spi.setDataSource(ds);
        spi.setCheckpointTableName("startstop_checkpoints");
        spi.setKeyFieldName("key");
        spi.setValueFieldName("value");
        spi.setValueFieldType("longvarbinary");
        spi.setExpireDateFieldName("expire_date");

        super.spiConfigure(spi);
    }
}