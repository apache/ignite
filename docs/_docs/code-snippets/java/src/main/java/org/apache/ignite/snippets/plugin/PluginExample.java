package org.apache.ignite.snippets.plugin;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.jupiter.api.Test;

public class PluginExample {
    
    @Test
    void registerPlugin() {
        //tag::example[]
        //tag::register-plugin[]
        IgniteConfiguration cfg = new IgniteConfiguration();

        //register a plugin that prints the cache size information every 100 seconds 
        cfg.setPluginProviders(new MyPluginProvider(100));

        //start the node
        Ignite ignite = Ignition.start(cfg);
        //end::register-plugin[]
        
        //tag::access-plugin[]
        //get an instance of the plugin
        MyPlugin p = ignite.plugin("MyPlugin");
        
        //print the cache size information
        p.printCacheInfo();
        //end::access-plugin[]
        
        IgniteCache cache = ignite.getOrCreateCache(new CacheConfiguration("test_cache").setBackups(1));
        
        for (int i = 0; i < 10; i++) {
           cache.put(i, "value " + i); 
        }

        //print the cache size information
        p.printCacheInfo();
        //end::example[]
        ignite.close();
    }
    
    public static void main(String[] args) {
       PluginExample pe = new PluginExample(); 
       pe.registerPlugin();
    }
}

