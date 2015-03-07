<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Data loading usually has to do with initializing cache data on startup. Using standard cache `put(...)` or `putAll(...)` operations is generally inefficient for loading large amounts of data. 
[block:api-header]
{
  "type": "basic",
  "title": "IgniteDataLoader"
}
[/block]
For fast loading of large amounts of data Ignite provides a utility interface, `IgniteDataLoader`, which internally will properly batch keys together and collocate those batches with nodes on which the data will be cached. 

The high loading speed is achieved with the following techniques:
  * Entries that are mapped to the same cluster member will be batched together in a buffer.
  * Multiple buffers can coexist at the same time.
  * To avoid running out of memory, data loader has a maximum number of buffers it can process concurrently.

To add data to the data loader, you should call `IgniteDataLoader.addData(...)` method.
[block:code]
{
  "codes": [
    {
      "code": "// Get the data loader reference and load data.\ntry (IgniteDataLoader<Integer, String> ldr = ignite.dataLoader(\"myCache\")) {    \n    // Load entries.\n    for (int i = 0; i < 100000; i++)\n        ldr.addData(i, Integer.toString(i));\n}",
      "language": "java"
    }
  ]
}
[/block]
## Allow Overwrite
By default, the data loader will only support initial data loading, which means that if it will encounter an entry that is already in cache, it will skip it. This is the most efficient and performant mode, as the data loader does not have to worry about data versioning in the background.

If you anticipate that the data may already be in the cache and you need to overwrite it, you should set `IgniteDataLoader.allowOverwrite(true)` parameter.

## Using Updater
For cases when you need to execute some custom logic instead of just adding new data, you can take advantage of `IgniteDataLoader.Updater` API. 

In the example below, we  generate random numbers and store them as key. The number of times the same number is generated is stored as value. The `Updater` helps to increment the value by 1 each time we try to load that same key into the cache.
[block:code]
{
  "codes": [
    {
      "code": "// Closure that increments passed in value.\nfinal GridClosure<Long, Long> INC = new GridClosure<Long, Long>() {\n    @Override public Long apply(Long e) {\n        return e == null ? 1L : e + 1;\n    }\n};\n\n// Get the data loader reference and load data.\ntry (GridDataLoader<Integer, String> ldr = grid.dataLoader(\"myCache\")) {   \n    // Configure the updater.\n    ldr.updater((cache, entries) -> {\n      for (Map.Entry<Integer, Long> e : entries)\n        cache.invoke(e.getKey(), (entry, args) -> {\n          Integer val = entry.getValue();\n\n          entry.setValue(val == null ? 1 : val + 1);\n\n          return null;\n        });\n    });\n \n    for (int i = 0; i < CNT; i++)\n        ldr.addData(RAND.nextInt(100), 1L);\n}",
      "language": "java",
      "name": "updater"
    },
    {
      "code": "// Closure that increments passed in value.\nfinal GridClosure<Long, Long> INC = new GridClosure<Long, Long>() {\n    @Override public Long apply(Long e) {\n        return e == null ? 1L : e + 1;\n    }\n};\n\n// Get the data loader reference and load data.\ntry (GridDataLoader<Integer, String> ldr = grid.dataLoader(\"myCache\")) {   \n    // Configure updater.\n    ldr.updater(new GridDataLoadCacheUpdater<Integer, Long>() {\n        @Override public void update(GridCache<Integer, Long> cache,\n            Collection<Map.Entry<Integer, Long>> entries) throws GridException {\n                for (Map.Entry<Integer, Long> e : entries)\n                    cache.transform(e.getKey(), INC);\n        }\n    });\n \n    for (int i = 0; i < CNT; i++)\n        ldr.addData(RAND.nextInt(100), 1L);\n}",
      "language": "java",
      "name": "java7 updater"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "IgniteCache.loadCache()"
}
[/block]
Another way to load large amounts of data into cache is through [CacheStore.loadCache()](docs/persistent-store#loadcache-) method, which allows for cache data loading even without passing all the keys that need to be loaded. 

`IgniteCache.loadCache()` method will delegate to `CacheStore.loadCache()` method on every cluster member that is running the cache. To invoke loading only on the local cluster node, use `IgniteCache.localLoadCache()` method.
[block:callout]
{
  "type": "info",
  "body": "In case of partitioned caches, keys that are not mapped to this node, either as primary or backups, will be automatically discarded by the cache."
}
[/block]
Here is an example of how `CacheStore.loadCache()` implementation. For a complete example of how a `CacheStore` can be implemented refer to [Persistent Store](doc:persistent-store).
[block:code]
{
  "codes": [
    {
      "code": "public class CacheJdbcPersonStore extends CacheStoreAdapter<Long, Person> {\n\t...\n  // This mehtod is called whenever \"IgniteCache.loadCache()\" or\n  // \"IgniteCache.localLoadCache()\" methods are called.\n  @Override public void loadCache(IgniteBiInClosure<Long, Person> clo, Object... args) {\n    if (args == null || args.length == 0 || args[0] == null)\n      throw new CacheLoaderException(\"Expected entry count parameter is not provided.\");\n\n    final int entryCnt = (Integer)args[0];\n\n    Connection conn = null;\n\n    try (Connection conn = connection()) {\n      try (PreparedStatement st = conn.prepareStatement(\"select * from PERSONS\")) {\n        try (ResultSet rs = st.executeQuery()) {\n          int cnt = 0;\n\n          while (cnt < entryCnt && rs.next()) {\n            Person person = new Person(rs.getLong(1), rs.getString(2), rs.getString(3));\n\n            clo.apply(person.getId(), person);\n\n            cnt++;\n          }\n        }\n      }\n    }\n    catch (SQLException e) {\n      throw new CacheLoaderException(\"Failed to load values from cache store.\", e);\n    }\n  }\n  ...\n}",
      "language": "java"
    }
  ]
}
[/block]