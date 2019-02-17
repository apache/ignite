<Query Kind="Program">
  <NuGetReference>Apache.Ignite</NuGetReference>
  <Namespace>Apache.Ignite.Core</Namespace>
  <Namespace>Apache.Ignite.Core.Binary</Namespace>
  <Namespace>Apache.Ignite.Core.Cache.Configuration</Namespace>
  <Namespace>Apache.Ignite.Core.Cache.Query</Namespace>
  <Namespace>Apache.Ignite.Core.Compute</Namespace>
  <Namespace>Apache.Ignite.Core.Deployment</Namespace>
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
/// Example demonstrating closure execution.
/// 
/// Requirements:
/// * Java Runtime Environment (JRE): http://www.oracle.com/technetwork/java/javase/downloads/index.html (x86 for regular LINQPad, x64 for AnyCPU LINQPad)
/// </summary>

void Main()
{
	// Force new LINQPad query process to reinit JVM
	Util.NewProcess = true;

	// Enable peer assembly loading.
	// This example can be run with standalone nodes started with Apache.Ignite.exe.
	// To download and start a standalone node from NuGet:
	// 1) > nuget install Apache.Ignite
	// 2) > cd Apache.Ignite*\lib\net40
	// 3) Enable peer assembly loading in Apache.Ignite.exe.config: 
	//    <igniteConfiguration peerAssemblyLoadingMode='CurrentAppDomain'>
	// 4) Run Apache.Ignite.exe	(one or more times)
	// 5) Start this example, check output in the standalone node`s console	
	var cfg = new IgniteConfiguration 
	{
		PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain 
	};
	
	// Start instance
    using (var ignite = Ignition.Start(cfg))
	{
		// Split the string by spaces to count letters in each word in parallel.
		var words = "Count characters using closure".Split();

		var res = ignite.GetCompute().Apply(new CharacterCountClosure(), words);

		int totalLen = res.Sum();  // reduce manually
		
		totalLen.Dump("Total character count with manual reduce");
		
		totalLen = ignite.GetCompute().Apply(new CharacterCountClosure(), words, new CharacterCountReducer());

		totalLen.Dump("Total character count with reducer");
	}
}

/// <summary>
/// Closure counting characters in a string.
/// </summary>
public class CharacterCountClosure : IComputeFunc<string, int>
{
	/// <summary>
	/// Calculate character count of the given word.
	/// </summary>
	/// <param name="arg">Word.</param>
	/// <returns>Character count.</returns>
	public int Invoke(string arg)
	{
		int len = arg.Length;

		Console.WriteLine("Character count in word \"" + arg + "\": " + len);

		return len;
	}
}

/// <summary>
/// Character count reducer which collects individual string lengths and aggregate them.
/// </summary>
public class CharacterCountReducer : IComputeReducer<int, int>
{
	/// <summary> Total length. </summary>
	private int _length;

	/// <summary>
	/// Collect character counts of distinct words.
	/// </summary>
	/// <param name="res">Character count of a distinct word.</param>
	/// <returns><c>True</c> to continue collecting results until all closures are finished.</returns>
	public bool Collect(int res)
	{
		_length += res;

		return true;
	}

	/// <summary>
	/// Reduce all collected results.
	/// </summary>
	/// <returns>Total character count.</returns>
	public int Reduce()
	{
		return _length;
	}
}