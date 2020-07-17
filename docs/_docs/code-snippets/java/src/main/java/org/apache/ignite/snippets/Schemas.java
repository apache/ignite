package org.apache.ignite.snippets;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.junit.jupiter.api.Test;

public class Schemas {

    @Test
    void config() {
        //tag::custom-schemas[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        SqlConfiguration sqlCfg = new SqlConfiguration();

        sqlCfg.setSqlSchemas("sqlSchemas");
        
        cfg.setSqlConfiguration(sqlCfg);
        
        //end::custom-schemas[]
    }
}
