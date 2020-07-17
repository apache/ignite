using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cluster;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class MapReduceApi
    {
        // tag::mapReduceComputeTask[]
        // tag::computeTaskExample[]
        class CharCountComputeJob : IComputeJob<int>
        {
            private readonly string _arg;

            public CharCountComputeJob(string arg)
            {
                Console.WriteLine(">>> Printing '" + arg + "' from compute job.");
                this._arg = arg;
            }

            public int Execute()
            {
                return _arg.Length;
            }

            public void Cancel()
            {
                throw new System.NotImplementedException();
            }
        }
        
        // end::computeTaskExample[]

        class CharCountTask : IComputeTask<string, int, int>
        {
            public IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, string arg)
            {
                var map = new Dictionary<IComputeJob<int>, IClusterNode>();
                using (var enumerator = subgrid.GetEnumerator())
                {
                    foreach (var s in arg.Split(" "))
                    {
                        if (!enumerator.MoveNext())
                        {
                            enumerator.Reset();
                            enumerator.MoveNext();
                        }

                        map.Add(new CharCountComputeJob(s), enumerator.Current);
                    }
                }

                return map;
            }

            public ComputeJobResultPolicy OnResult(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
            {
                // If there is no exception, wait for all job results.
                return res.Exception != null ? ComputeJobResultPolicy.Failover : ComputeJobResultPolicy.Wait;
            }

            public int Reduce(IList<IComputeJobResult<int>> results)
            {
                return results.Select(res => res.Data).Sum();
            }
        }

        public static void MapReduceComputeJobDemo()
        {
            var ignite = Ignition.Start(new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalPort = 48500,
                    LocalPortRange = 20,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:48500..48520"
                        }
                    }
                }
            });

            var compute = ignite.GetCompute();

            var res = compute.Execute(new CharCountTask(), "Hello Grid Please Count Chars In These Words");

            Console.WriteLine("res=" + res);
        }
        // end::mapReduceComputeTask[]

        // tag::computeTaskExample[]
        public class ComputeTaskExample
        {
            private class CharacterCountTask : ComputeTaskSplitAdapter<string, int, int>
            {
                public override int Reduce(IList<IComputeJobResult<int>> results)
                {
                    return results.Select(res => res.Data).Sum();
                }

                protected override ICollection<IComputeJob<int>> Split(int gridSize, string arg)
                {
                    return arg.Split(" ")
                        .Select(word => new CharCountComputeJob(word))
                        .Cast<IComputeJob<int>>()
                        .ToList();
                }
            }

            public static void RunComputeTaskExample()
            {
                var ignite = Ignition.Start(new IgniteConfiguration
                {
                    DiscoverySpi = new TcpDiscoverySpi
                    {
                        LocalPort = 48500,
                        LocalPortRange = 20,
                        IpFinder = new TcpDiscoveryStaticIpFinder
                        {
                            Endpoints = new[]
                            {
                                "127.0.0.1:48500..48520"
                            }
                        }
                    }
                });

                var cnt = ignite.GetCompute().Execute(new CharacterCountTask(), "Hello Grid Enabled World!");
                Console.WriteLine(">>> Total number of characters in the phrase is '" + cnt + "'.");
            }
        }
        // end::computeTaskExample[]
    }
}