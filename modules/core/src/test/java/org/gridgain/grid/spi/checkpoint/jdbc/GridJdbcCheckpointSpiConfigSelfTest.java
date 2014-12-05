/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.jdbc;

import org.gridgain.testframework.junits.spi.*;
import org.hsqldb.jdbc.*;
import javax.sql.*;

import static org.gridgain.grid.spi.checkpoint.jdbc.JdbcCheckpointSpi.*;

/**
 * Grid jdbc checkpoint SPI config self test.
 */
@GridSpiTest(spi = JdbcCheckpointSpi.class, group = "Checkpoint SPI")
public class GridJdbcCheckpointSpiConfigSelfTest extends GridSpiAbstractConfigTest<JdbcCheckpointSpi> {
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
