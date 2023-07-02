package com.shard.jdbc.plugin;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

import java.util.Map;

import com.google.common.collect.ImmutableList;

import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ConnectorFactory;



public class ShardingJdbcPlugin implements Plugin {

	private final String  connencter_id;
	private final ShardingJdbcClientModule module;
	
    public ShardingJdbcPlugin() {
        // name of the connector and the module implementation       
        this.connencter_id = "sharding-jdbc";
        this.module = new ShardingJdbcClientModule();
    }
    
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {    	
        return ImmutableList.of(new ShardingJdbcConnectorFactory(this.connencter_id, module));
    }
    
}
