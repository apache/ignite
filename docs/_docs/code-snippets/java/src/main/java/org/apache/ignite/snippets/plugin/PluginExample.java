/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

