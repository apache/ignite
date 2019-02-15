<Query Kind="Program">
    <NuGetReference>Apache.Ignite</NuGetReference>
    <Namespace>Apache.Ignite.Core</Namespace>
    <Namespace>Apache.Ignite.Core.Binary</Namespace>
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
        // Create new cache
        var cache = ignite.GetOrCreateCache<int, Organization>("orgs");

        // Put data entry to cache
        cache.Put(1, new Organization {Name = "Apache", Type="Private"});

        // Retrieve data entry in fully deserialized form
        cache.Get(1).Dump("Retrieved organization instance from cache");

        // Create projection that will get values as binary objects
        var binaryCache = cache.WithKeepBinary<int, IBinaryObject>();

        // Get recently created organization as a binary object
        var binaryOrg = binaryCache.Get(1);

        // Get organization's name from binary object (note that object doesn't need to be fully deserialized)
        binaryOrg.GetField<string>("name").Dump("Retrieved organization name from binary object");
	}
}

public class Organization
{
	public string Name { get; set; }
	
	public string Type { get; set; }
}