package com.shard.jdbc.plugin;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;

import io.airlift.bootstrap.LifeCycleManager;
import io.prestosql.plugin.jdbc.JdbcConnector;
import io.prestosql.plugin.jdbc.JdbcMetadataFactory;
import io.prestosql.plugin.jdbc.SessionPropertiesProvider;
import io.prestosql.spi.connector.ConnectorAccessControl;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.procedure.Procedure;
import io.prestosql.spi.session.PropertyMetadata;

public class ShardingJdbcConnector extends JdbcConnector{
	public static final String PRIMARY_KEY = "primary_key";
	
	@Inject
    public ShardingJdbcConnector(
            LifeCycleManager lifeCycleManager,
            JdbcMetadataFactory jdbcMetadataFactory,
            ConnectorSplitManager jdbcSplitManager,
            ConnectorRecordSetProvider jdbcRecordSetProvider,
            ConnectorPageSinkProvider jdbcPageSinkProvider,
            Optional<ConnectorAccessControl> accessControl,
            Set<Procedure> procedures,
            Set<SessionPropertiesProvider> sessionProperties)
    {
		super(lifeCycleManager, jdbcMetadataFactory, jdbcSplitManager, jdbcRecordSetProvider, jdbcPageSinkProvider, accessControl, procedures, sessionProperties);
    }
	

    /** 
     * add@byron support ignite with property
     * @return the table properties for this connector
     */
	@Override
    public List<PropertyMetadata<?>> getTableProperties()
    {
    	return Arrays.asList(
    			PropertyMetadata.stringProperty(PRIMARY_KEY, "setting primay key column of table", null, false),
    			PropertyMetadata.stringProperty("affinity_key", "setting affinity_key column of table", null, false),
    			PropertyMetadata.stringProperty("template", "setting template of table", null, true),
    			PropertyMetadata.stringProperty("cache_name", "setting cache name of table", null, true)
    			
    			);
    }

    /**add@byron
     * @return the column properties for this connector
     */
	@Override
    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return Arrays.asList(PropertyMetadata.stringProperty(PRIMARY_KEY, "setting column is primay key", null, false));
    }
}