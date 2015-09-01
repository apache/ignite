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

package org.apache.ignite.spi.checkpoint.jdbc;

import javax.sql.DataSource;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.hsqldb.jdbc.jdbcDataSource;

import static org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi.DFLT_CHECKPOINT_TABLE_NAME;
import static org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi.DFLT_EXPIRE_DATE_FIELD_NAME;
import static org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi.DFLT_KEY_FIELD_NAME;
import static org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi.DFLT_KEY_FIELD_TYPE;
import static org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi.DFLT_VALUE_FIELD_NAME;
import static org.apache.ignite.spi.checkpoint.jdbc.JdbcCheckpointSpi.DFLT_VALUE_FIELD_TYPE;

/**
 * Grid jdbc checkpoint SPI config self test.
 */
@GridSpiTest(spi = JdbcCheckpointSpi.class, group = "Checkpoint SPI")
public class JdbcCheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<JdbcCheckpointSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new JdbcCheckpointSpi(), "dataSource", null);

        DataSource ds = new jdbcDataSource();

        JdbcCheckpointSpi spi = new JdbcCheckpointSpi();

        spi.setDataSource(ds);

        checkNegativeSpiProperty(spi, "checkpointTableName", null);
        checkNegativeSpiProperty(spi, "checkpointTableName", "");

        spi.setCheckpointTableName(DFLT_CHECKPOINT_TABLE_NAME);

        checkNegativeSpiProperty(spi, "keyFieldName", null);
        checkNegativeSpiProperty(spi, "keyFieldName", "");

        spi.setKeyFieldName(DFLT_KEY_FIELD_NAME);

        checkNegativeSpiProperty(spi, "keyFieldType", null);
        checkNegativeSpiProperty(spi, "keyFieldType", "");

        spi.setKeyFieldType(DFLT_KEY_FIELD_TYPE);

        checkNegativeSpiProperty(spi, "valueFieldName", null);
        checkNegativeSpiProperty(spi, "valueFieldName", "");

        spi.setValueFieldName(DFLT_VALUE_FIELD_NAME);

        checkNegativeSpiProperty(spi, "valueFieldType", null);
        checkNegativeSpiProperty(spi, "valueFieldType", "");

        spi.setValueFieldType(DFLT_VALUE_FIELD_TYPE);

        checkNegativeSpiProperty(spi, "expireDateFieldName", null);
        checkNegativeSpiProperty(spi, "expireDateFieldName", "");

        spi.setExpireDateFieldName(DFLT_EXPIRE_DATE_FIELD_NAME);

        checkNegativeSpiProperty(spi, "expireDateFieldType", null);
        checkNegativeSpiProperty(spi, "expireDateFieldType", "");
    }
}