package com.shard.jdbc.plugin;



import static io.airlift.configuration.ConfigBinder.configBinder;

import java.lang.reflect.Constructor;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.shard.jdbc.util.DbUtil;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;


public class ShardingJdbcClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(ShardingJdbcClient.class).in(Scopes.SINGLETON);
        
        binder.bind(ShardingJdbcSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(ShardingJdbcConnector.class).in(Scopes.SINGLETON);
        
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ShardingJdbcConfig.class);
        
        
    }    
    
    

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, ShardingJdbcConfig metaDataConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", Boolean.toString(true));
        connectionProperties.setProperty("useSSL", "false");
        connectionProperties.setProperty("useUnicode", "true");
        
        DbUtil.init(metaDataConfig.getShardingRulePath());

        basicConnectionProperties(connectionProperties,metaDataConfig);
        
        Driver driver = setupDriver(metaDataConfig.getDriver());
        return new DriverConnectionFactory(
        		driver, 
        		config.getConnectionUrl(),         		
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
    

	public static Driver setupDriver(String driverClassName) {
		
		try {
			//Driver driver = org.postgresql.Driver();
			Class<Driver> cls = (Class<Driver>) Class.forName(driverClassName);
			Constructor<Driver> con = cls.getConstructor();
			con.setAccessible(true);
			return con.newInstance();
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException(String.valueOf(e.getMessage()));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IllegalStateException(String.valueOf(e.getMessage()));
		}

	}

}
