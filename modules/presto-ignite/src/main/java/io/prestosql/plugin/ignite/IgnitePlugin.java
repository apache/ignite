package io.prestosql.plugin.ignite;

import io.prestosql.plugin.jdbc.JdbcPlugin;


public class IgnitePlugin extends JdbcPlugin {

    public IgnitePlugin() {
        // name of the connector and the module implementation
        super("ignite", new IgniteClientModule());
    }
}
