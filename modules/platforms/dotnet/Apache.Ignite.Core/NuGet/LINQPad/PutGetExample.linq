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
/// </summary>

// NOTE: x64 LINQPad is required to run this sample (see AnyCPU build: http://www.linqpad.net/Download.aspx)

void Main()
{
using (var ignite = Ignition.Start())
{
        ">>> Ignite put-get example started".Dump();

        var cache = ignite.CreateCache<int, Organization="">("orgs");

		cache.Put(1, new Organization {Name = "Apache", Type="Non-Profit"});
	
		cache.Get(1).Dump("Retrieved organization instance from cache");
	}
}

[Serializable]
public class Organization
{
	public string Name { get; set; }
	
	public string Type { get; set; }
}