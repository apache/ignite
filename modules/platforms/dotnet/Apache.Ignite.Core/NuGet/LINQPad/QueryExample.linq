<Query Kind="Program">
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
/// This example demonstrates put-get operations on Ignite cache
/// with binary values. Note that binary object can be retrieved in
/// fully-deserialized form or in binary object format using special
/// cache projection.
/// 
/// Requirements:
/// * Java Runtime Environment (JRE): http://www.oracle.com/technetwork/java/javase/downloads/index.html (x86 for regular LINQPad, x64 for AnyCPU LINQPad)
/// </summary>

void Main()
{
	// Force new LINQPad query process to reinit JVM
	Util.NewProcess = true;
	
    // Start instance
    using (var ignite = Ignition.Start())
    {
        // Create and populate organization cache
        var orgs = ignite.GetOrCreateCache<int, Organization>(new CacheConfiguration("orgs-sql", 
			new QueryEntity(typeof(int), typeof(Organization))));
        orgs[1] = new Organization { Name = "Apache", Type = "Private", Size = 5300 };
        orgs[2] = new Organization { Name = "Microsoft", Type = "Private", Size = 110000 };
        orgs[3] = new Organization { Name = "Red Cross", Type = "Non-Profit", Size = 35000 };

        // Create and populate person cache
        var persons = ignite.GetOrCreateCache<int, Person>(new CacheConfiguration("persons-sql", typeof(Person)));
        persons[1] = new Person { OrgId = 1, Name = "James Wilson" };
        persons[2] = new Person { OrgId = 1, Name = "Daniel Adams" };
        persons[3] = new Person { OrgId = 2, Name = "Christian Moss" };
        persons[4] = new Person { OrgId = 3, Name = "Allison Mathis" };
		persons[5] = new Person { OrgId = 3, Name = "Christopher Adams" };

        // SQL query
        orgs.Query(new SqlQuery(typeof(Organization), "size < ?", 100000)).Dump("Organizations with size less than 100K");
		
		// SQL query with join
		const string orgName = "Apache";
		persons.Query(new SqlQuery(typeof(Person), "from Person, \"orgs-sql\".Organization where Person.OrgId = \"orgs-sql\".Organization._key and \"orgs-sql\".Organization.Name = ?", orgName))
			.Dump("Persons working for " + orgName);

		// Fields query
		orgs.Query(new SqlFieldsQuery("select name, size from Organization")).Dump("Fields query");

		// Full text query
		persons.Query(new TextQuery(typeof(Person), "Chris*")).Dump("Persons starting with 'Chris'");
	}
}

public class Organization
{
	[QuerySqlField]
	public string Name { get; set; }
	
	public string Type { get; set; }

	[QuerySqlField]
	public int Size { get; set;}
}

public class Person
{
	[QueryTextField]
	public string Name { get; set; }

	[QuerySqlField]
	public int OrgId { get; set; }
}