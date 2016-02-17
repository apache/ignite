<Query Kind="Statements">
  <Reference>Apache.Ignite.Core.dll</Reference>
  <Namespace>Apache.Ignite.Core</Namespace>
</Query>

// NOTE: x64 LINQPad is required to run this sample (see AnyCPU build: http://www.linqpad.net/Download.aspx)

using (var ignite = Ignition.Start())
{
var cache = ignite.CreateCache<int, string>("test");
	
	cache[1] = "111";
	
	cache[1].Dump();
}