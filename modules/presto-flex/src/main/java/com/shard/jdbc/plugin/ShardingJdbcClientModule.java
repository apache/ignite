package com.shard.jdbc.plugin;



import static io.airlift.configuration.ConfigBinder.configBinder;

import java.sql.SQLException;
import java.util.Properties;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;


public class ShardingJdbcClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ShardingJdbcClient.class).in(Scopes.SINGLETON);
        
        binder.bind(ShardingJdbcSinkProvider.class).in(Scopes.SINGLETON);
        
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ShardingJdbcConfig.class);
        
        
    }    
    
    

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, ShardingJdbcConfig shardingJdbcConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", Boolean.toString(true));
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        

        basicConnectionProperties(connectionProperties,shardingJdbcConfig);
        
        return new ShardingDriverConnectionFactory(
        		shardingJdbcConfig,
                config,
                connectionProperties,
                credentialProvider);
    }
    

    public static Properties basicConnectionProperties(Properties connectionProperties,ShardingJdbcConfig config)
    {
       
        if (config.getUser() != null) {
            connectionProperties.setProperty("user", config.getUser());
        }
        if (config.getPassword() != null) {
            connectionProperties.setProperty("password", config.getPassword());
        }
        return connectionProperties;
    }

}
