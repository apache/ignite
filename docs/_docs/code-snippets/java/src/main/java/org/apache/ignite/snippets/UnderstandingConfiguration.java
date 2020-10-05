package org.apache.ignite.snippets;

import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class UnderstandingConfiguration {

    public static void configurationDemo() {
        //tag::cfg[]
        //tag::dir[]
        IgniteConfiguration igniteCfg = new IgniteConfiguration();
        //end::dir[]
        //setting a work directory
        //tag::dir[]
        igniteCfg.setWorkDirectory("/path/to/work/directory");
        //end::dir[]

        //defining a partitioned cache
        CacheConfiguration cacheCfg = new CacheConfiguration("myCache");
        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        igniteCfg.setCacheConfiguration(cacheCfg);
        //end::cfg[]
    }
}
