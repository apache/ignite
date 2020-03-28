package com.facebook.presto.plugin.ignite;

import com.facebook.presto.plugin.jdbc.JdbcPlugin;


public class IgnitePlugin extends JdbcPlugin {

    public IgnitePlugin() {
        // name of the connector and the module implementation
        super("ignite", new IgniteClientModule());
    }
}
