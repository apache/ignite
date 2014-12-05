/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.checkpoint.jdbc;

import org.apache.ignite.spi.checkpoint.*;
import org.gridgain.testframework.junits.spi.*;
import org.hsqldb.jdbc.*;

/**
 * Grid jdbc checkpoint SPI default config self test.
 */
@GridSpiTest(spi = JdbcCheckpointSpi.class, group = "Checkpoint SPI")
public class GridJdbcCheckpointSpiDefaultConfigSelfTest extends
    GridCheckpointSpiAbstractTest<JdbcCheckpointSpi> {
    /** {@inheritDoc} */
    @Override protected void spiConfigure(JdbcCheckpointSpi spi) throws Exception {
        jdbcDataSource ds = new jdbcDataSource();

        ds.setDatabase("jdbc:hsqldb:mem:gg_test_" + getClass().getSimpleName());
        ds.setUser("sa");
        ds.setPassword("");

        spi.setDataSource(ds);

        // Default BLOB type is not valid for hsqldb.
        spi.setValueFieldType("longvarbinary");

        super.spiConfigure(spi);
    }
}
