<Query Kind="Statements">
  <NuGetReference>Apache.Ignite</NuGetReference>
  <Namespace>Apache.Ignite.Core</Namespace>
  <Namespace>Apache.Ignite.Core.Client</Namespace>
  <Namespace>Apache.Ignite.Core.Binary</Namespace>
  <Namespace>Apache.Ignite.Core.Cache.Query</Namespace>
</Query>

/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
