package org.apache.ignite.snippets.plugin;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class PluginExample {
    
    public static void main(String[] args) {
       new PluginExample().specifyPlugin();  
    }

    @Test
    void specifyPlugin() {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setPluginProviders(new MyPluginProvider());

        Ignite ignite = Ignition.start(cfg);
        
        MyPlugin p = ignite.plugin("MyPlugin");
    }
}
