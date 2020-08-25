package org.apache.ignite.snippets.plugin;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;

/**
 * The plugin prints cache size information to console  
 * @author 
 *
 */
public class MyPlugin implements IgnitePlugin, Runnable {
    
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    private PluginContext context;

    private long period;

    /**
     * 
     * @param context 
     */
    public MyPlugin(long period, PluginContext context) {
        this.period = period;
        this.context = context;
    }

    private void print0() {
        StringBuilder sb = new StringBuilder("\nCache Information: \n");

        //get the names of all caches
        context.grid().cacheNames().forEach(cacheName -> {
            //get the specific cache
            IgniteCache cache = context.grid().cache(cacheName);
            if (cache != null) {
                sb.append("  cacheName=").append(cacheName).append(", size=").append(cache.size())
                        .append("\n");
            }
        });

        System.out.print(sb.toString());
    }

    /**
     * Prints the information about caches to console.
     */
    public void printCacheInfo() {
        print0();
    }

    @Override
    public void run() {
        print0();
    }
    
    void start() {
        scheduler.scheduleAtFixedRate(this, period, period, TimeUnit.SECONDS);
    }
    
    void stop() {
       scheduler.shutdownNow(); 
    }
}
