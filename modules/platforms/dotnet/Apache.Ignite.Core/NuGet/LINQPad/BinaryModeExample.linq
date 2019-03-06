<Query Kind="Statements">
  <NuGetReference>Apache.Ignite</NuGetReference>
  <Namespace>Apache.Ignite.Core</Namespace>
  <Namespace>Apache.Ignite.Core.Binary</Namespace>
  <Namespace>Apache.Ignite.Core.Cache.Configuration</Namespace>
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
/// This example works with cache entirely in binary mode: no classes are needed.
/// 
/// Requirements:
/// * Java Runtime Environment (JRE): http://www.oracle.com/technetwork/java/javase/downloads/index.html (x86 for regular LINQPad, x64 for AnyCPU LINQPad)
/// </summary>

// Force new LINQPad query process to reinit JVM.
Util.NewProcess = true;

// Start instance.
using (var ignite = Ignition.Start())
{	
    // Create new cache and configure queries for Person binary type.
    var cache0 = ignite.GetOrCreateCache<object, object>(new CacheConfiguration
    {
        Name = "persons",
        QueryEntities = new[]
        {
            new QueryEntity
            {
                KeyType = typeof(int),
                ValueTypeName = "Person",
                Fields = new[]
                {
                    new QueryField("Name", typeof(string)),
                    new QueryField("Age", typeof(int))
                }
            },
        }
    });

	// Switch to binary mode to work with data in serialized form.
	var cache = cache0.WithKeepBinary<int, IBinaryObject>();
	
	// Populate cache by creating objects with binary builder.
	var binary = cache.Ignite.GetBinary();

	cache[1] = binary.GetBuilder("Person")
		.SetField("Name", "James Wilson").SetField("Age", 23).Build();

	cache[2] = binary.GetBuilder("Person")
		.SetField("Name", "Daniel Adams").SetField("Age", 56).Build();

	cache[3] = binary.GetBuilder("Person")
		.SetField("Name", "Cristian Moss").SetField("Age", 40).Build();

	cache[4] = binary.GetBuilder("Person")
		.SetField("Name", "Allison Mathis").SetField("Age", 32).Build();
		
	// Read a cache entry field in binary mode.
	var person = cache[1];
	
	var name = person.GetField<string>("Name");	
	name.Dump("Name of the person with id 1:");
	
	// Modify an entry.
	cache[1] = person.ToBuilder().SetField("Name", name + " Jr.").Build();
	cache[1].ToString().Dump("Modified person with id 1:");
	
	// Run SQL query.
	cache.Query(new SqlQuery("Person", "age < 40"))
		.Select(x => x.Value.ToString())
		.Dump("Persons with age less than 40:");
		
	// Run SQL fields query.
	cache.Query(new SqlFieldsQuery("select name from Person order by name"))
		.Dump("All person names:");
}