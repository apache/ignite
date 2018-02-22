<Query Kind="Program">
    <NuGetReference>Apache.Ignite</NuGetReference>
    <Namespace>Apache.Ignite.Core</Namespace>
    <Namespace>Apache.Ignite.Core.Binary</Namespace>
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