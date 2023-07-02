package com.shard.jdbc.plugin;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import com.google.inject.Injector;
import com.google.inject.Module;


import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;

import io.prestosql.plugin.jdbc.JdbcConnector;
import io.prestosql.plugin.jdbc.JdbcConnectorFactory;
import io.prestosql.plugin.jdbc.JdbcHandleResolver;
import io.prestosql.plugin.jdbc.JdbcMetadataFactory;
import io.prestosql.plugin.jdbc.JdbcModule;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TypeManager;


public class ShardingJdbcConnectorFactory extends JdbcConnectorFactory{
	
	  
	public static  class ShardingJdbcHandleResolver extends JdbcHandleResolver{
    	 	@Override
    	    public Class<? extends ConnectorSplit> getSplitClass(){
    	        return ShardingJdbcSplit.class;
    	    }
    }
    
	

 	private final String name;
    private final JdbcModuleProvider moduleProvider;

    public ShardingJdbcConnectorFactory(String name, Module module)
    {
        this(name, JdbcModuleProvider.withCredentialProvider(module));
    }

    public ShardingJdbcConnectorFactory(String name, JdbcModuleProvider moduleProvider)
    {	
    	super(name,moduleProvider);    	        
        this.name = name;
        this.moduleProvider = requireNonNull(moduleProvider, "moduleProvider is null");
    }

    @Override
    public String getName()
    {
        return name;
    }
	
	
	@Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new ShardingJdbcHandleResolver();
    }
	

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");

        Bootstrap app = new Bootstrap(
                binder -> binder.bind(TypeManager.class).toInstance(context.getTypeManager()),
                binder -> binder.bind(NodeManager.class).toInstance(context.getNodeManager()),
                binder -> binder.bind(VersionEmbedder.class).toInstance(context.getVersionEmbedder()),
                new JdbcModule(catalogName),
                moduleProvider.getModule(catalogName));

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(requiredConfig)
                .initialize();

        return injector.getInstance(ShardingJdbcConnector.class);
    }

}
