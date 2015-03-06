Ignite automatically groups or batches event notifications that are generated as a result of cache events occurring within the cluster.

Each activity in cache can result in an event notification being generated and sent. For systems where cache activity is high, getting notified for every event could be network intensive, possibly leading to a decreased performance of cache operations in the grid.

In Ignite, event notifications can be grouped together and sent in batches or timely intervals. Here is an example of how this can be done:

[block:code]
{
  "codes": [
    {
      "code": "Ignite ignite = Ignition.ignite();\n \n// Get an instance of named cache.\nfinal IgniteCache<Integer, String> cache = ignite.jcache(\"cacheName\");\n \n// Sample remote filter which only accepts events for keys\n// that are greater than or equal to 10.\nIgnitePredicate<CacheEvent> rmtLsnr = new IgnitePredicate<CacheEvent>() {\n    @Override public boolean apply(CacheEvent evt) {\n        System.out.println(\"Cache event: \" + evt);\n \n        int key = evt.key();\n \n        return key >= 10;\n    }\n};\n \n// Subscribe to cache events occuring on all nodes \n// that have the specified cache running. \n// Send notifications in batches of 10.\nignite.events(ignite.cluster().forCacheNodes(\"cacheName\")).remoteListen(\n\t\t10 /*batch size*/, 0 /*time intervals*/, false, null, rmtLsnr, EVTS_CACHE);\n \n// Generate cache events.\nfor (int i = 0; i < 20; i++)\n    cache.put(i, Integer.toString(i));",
      "language": "java"
    }
  ]
}
[/block]