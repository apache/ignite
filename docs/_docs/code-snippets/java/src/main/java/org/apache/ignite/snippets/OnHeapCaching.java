package org.apache.ignite.snippets;

import org.apache.ignite.configuration.CacheConfiguration;

public class OnHeapCaching {

    public static void onHeapCacheExample() {
        //tag::onHeap[]
        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setName("myCache");
        cfg.setOnheapCacheEnabled(true);
        //end::onHeap[]

    }
}
