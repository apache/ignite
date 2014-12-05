/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.checkpoint.jdbc;

import org.gridgain.grid.spi.*;
import org.gridgain.testframework.junits.spi.*;
import org.hsqldb.jdbc.*;

/**
 * Grid jdbc checkpoint SPI start stop self test.
 */
@GridSpiTest(spi = JdbcCheckpointSpi.class, group = "Checkpoint SPI")
public class GridJdbcCheckpointSpiStartStopSelfTest
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
