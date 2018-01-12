<Query Kind="Statements">
  <NuGetReference>Apache.Ignite</NuGetReference>
  <Namespace>Apache.Ignite.Core</Namespace>
  <Namespace>Apache.Ignite.Core.Client</Namespace>
  <Namespace>Apache.Ignite.Core.Binary</Namespace>
  <Namespace>Apache.Ignite.Core.Cache.Query</Namespace>
</Query>

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

/// <summary>
/// This example demonstrates thin client protocol connection.
/// Thin client connects to an existing Ignite node and does not start JVM in process.
///
/// This example requires running Ignite node.
/// You can start an Ignite node with ignite.bat or Apache.Ignite.exe.
/// Alternatively, open QueryExample in a separate tab, add Thread.Sleep(-1) in there and run.
/// </summary>

var cfg = new IgniteClientConfiguration { Host = "127.0.0.1" };
using (var client = Ignition.StartClient(cfg))
{
	// Create cache for demo purpose.
	var fooCache = client.GetOrCreateCache<int, object>("thin-client-test").WithKeepBinary<int, IBinaryObject>();
	fooCache[1] = client.GetBinary().GetBuilder("foo")
		.SetStringField("Name", "John")
		.SetTimestampField("Birthday", new DateTime(2001, 5, 15).ToUniversalTime())
		.Build();

	var cacheNames = client.GetCacheNames();
	"Diplaying first 5 items from each cache:".Dump();
	
	foreach (var name in cacheNames)
	{
		var cache = client.GetCache<object, object>(name).WithKeepBinary<object, object>();
		var items = cache.Query(new ScanQuery<object, object>()).Take(5)
			.ToDictionary(x => x.Key.ToString(), x => x.Value.ToString());
		
		items.Dump(name);
	}
}
