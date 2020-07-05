package com.shard.jdbc.plugin;



import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.ConnectionFactory;
import com.facebook.presto.plugin.jdbc.DriverConnectionFactory;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scope;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;


public class ShardingJdbcClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).to(ShardingJdbcClient.class).in(Scopes.SINGLETON);
        
        binder.bind(ShardingJdbcSinkProvider.class).in(Scopes.SINGLETON);
        
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(ShardingJdbcConfig.class);
        
        
    }    
    
}
