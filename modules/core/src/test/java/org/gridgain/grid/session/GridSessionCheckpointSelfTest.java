/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.checkpoint.cache.*;
import org.gridgain.grid.spi.checkpoint.jdbc.*;
import org.gridgain.grid.spi.checkpoint.sharedfs.*;
import org.gridgain.testframework.junits.common.*;
import org.hsqldb.jdbc.*;

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

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);

        CacheCheckpointSpi spi = new CacheCheckpointSpi();

        spi.setCacheName(cacheName);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setCheckpointSpi(spi);

        GridSessionCheckpointSelfTest.spi = spi;

        checkCheckpoints(cfg);
    }
}
