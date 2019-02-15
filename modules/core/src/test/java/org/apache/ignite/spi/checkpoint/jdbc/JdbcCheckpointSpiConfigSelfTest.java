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

package org.apache.ignite.spi.checkpoint.jdbc;

import javax.sql.DataSource;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.hsqldb.jdbc.jdbcDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
@RunWith(JUnit4.class)
public class JdbcCheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<JdbcCheckpointSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
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
