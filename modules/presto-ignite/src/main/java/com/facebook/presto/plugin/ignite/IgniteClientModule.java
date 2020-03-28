package com.facebook.presto.plugin.ignite;

import com.facebook.presto.plugin.jdbc.BaseJdbcConfig;
import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scope;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;


public class IgniteClientModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(JdbcClient.class).to(IgniteClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(BaseJdbcConfig.class);
        configBinder(binder).bindConfig(IgniteConfig.class);
    }
}
