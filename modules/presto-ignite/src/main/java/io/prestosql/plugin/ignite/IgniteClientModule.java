package io.prestosql.plugin.ignite;



import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.ForBaseJdbc;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.spi.session.PropertyMetadata;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scope;
import com.google.inject.Scopes;
import com.google.inject.Singleton;


import static io.airlift.configuration.ConfigBinder.configBinder;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.ignite.IgniteJdbcDriver;
import org.apache.ignite.IgniteJdbcThinDriver;


import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.plugin.jdbc.JdbcModule.bindSessionPropertiesProvider;

public class IgniteClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(IgniteClient.class).in(Scopes.SINGLETON);
        
        binder.bind(IgnitePageSinkProvider.class).in(Scopes.SINGLETON);
        
        bindSessionPropertiesProvider(binder, IgniteSessionProperties.class);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(IgniteConfig.class);
        
        
    }    
    

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory createConnectionFactory(BaseJdbcConfig config, CredentialProvider credentialProvider, IgniteConfig igniteConfig)
            throws SQLException
    {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("useInformationSchema", Boolean.toString(true));
        connectionProperties.setProperty("nullCatalogMeansCurrent", "false");
        connectionProperties.setProperty("useUnicode", "true");
        connectionProperties.setProperty("characterEncoding", "utf8");
        connectionProperties.setProperty("tinyInt1isBit", "false");
        

        basicConnectionProperties(connectionProperties,igniteConfig);
        
        Driver driver = igniteConfig.isThinConnection() ? 
				 new IgniteJdbcThinDriver():new IgniteJdbcDriver();

				
        return new DriverConnectionFactory(driver,     	
                config.getConnectionUrl(),
                connectionProperties,
                credentialProvider);
    }
    

    public static Properties basicConnectionProperties(Properties connectionProperties,IgniteConfig config)
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
