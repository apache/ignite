package org.apache.ignite.cache;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cdc.SqlCdcTest;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cdc.AbstractCdcTest.JOHN;
import static org.apache.ignite.internal.cdc.SqlCdcTest.SARAH;
import static org.apache.ignite.internal.cdc.SqlCdcTest.USER;

/** */
public class IndexClearOnEvictionTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testIndexBatchClearing() throws Exception {
        IgniteEx ign = startGrids(2);

        SqlCdcTest.executeSql(
            ign,
            "CREATE TABLE USER(id int, city_id int, name varchar, PRIMARY KEY (id, city_id)) " +
                "WITH \"CACHE_NAME=" + USER + ", BACKUPS=1\""
        );

        awaitPartitionMapExchange();

        SqlCdcTest.executeSql(ign, "CREATE INDEX IDX1 ON USER(name)");

        for (int i = 0; i < 10_000; i++) {
            SqlCdcTest.executeSql(
                ign,
                "INSERT INTO USER VALUES(?, ?, ?)",
                i,
                42 * i,
                (i % 2 == 0 ? JOHN : SARAH) + i);
        }

        startGrid(2);

        awaitPartitionMapExchange();
    }
}
