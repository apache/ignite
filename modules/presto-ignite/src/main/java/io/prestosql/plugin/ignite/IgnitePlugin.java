package io.prestosql.plugin.ignite;

import com.google.common.collect.ImmutableList;
import com.shard.jdbc.plugin.ShardingJdbcClientModule;
import com.shard.jdbc.plugin.ShardingJdbcConnectorFactory;

import io.prestosql.plugin.jdbc.JdbcPlugin;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;


public class IgnitePlugin implements Plugin {

	private final String  connencter_id;
	private final IgniteClientModule module;
	
    public IgnitePlugin() {
        // name of the connector and the module implementation       
        this.connencter_id = "ignite";
        this.module = new IgniteClientModule();
    }
    
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {    	
        return ImmutableList.of(new ShardingJdbcConnectorFactory(this.connencter_id, module));
    }   
}
