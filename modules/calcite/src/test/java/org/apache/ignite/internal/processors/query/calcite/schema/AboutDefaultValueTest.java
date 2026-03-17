package org.apache.ignite.internal.processors.query.calcite.schema;

import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.indexing.IndexingQueryEngineConfiguration;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.junit.Test;

public class AboutDefaultValueTest extends AbstractBasicIntegrationTest {
    @Override protected int nodeCount() {
        return 1;
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration().setQueryEnginesConfiguration(
            new CalciteQueryEngineConfiguration().setDefault(true),
            new IndexingQueryEngineConfiguration()
        );

        return super.getConfiguration(igniteInstanceName)
            .setSqlConfiguration(sqlCfg);
    }

    @Test
    public void name() {
        sql("create table PERSON(id int primary key, birth_date TIMESTAMP DEFAULT (CURRENT_TIMESTAMP))");

        sql("insert into PERSON(id) values(?)", 1);

        log.info(">>>> select: " + sql("select * from PERSON order by id"));
    }
}
