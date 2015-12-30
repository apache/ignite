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

namespace Apache.Ignite.Core.Tests.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Cancellation tests.
    /// </summary>
    public class CancellationTest : IgniteTestBase
    {
        // TODO: 
        // Java task
        // C# task
        // Closures

        public CancellationTest() 
            : base("config\\compute\\compute-grid1.xml", "config\\compute\\compute-grid2.xml")
        {
            // No-op.
        }

        [Test]
        public void TestTask()
        {
            Console.WriteLine("Test: " + Thread.CurrentThread.ManagedThreadId);
            var compute = Compute;

            var cts = new CancellationTokenSource();
            var task = compute.ExecuteAsync(new Task(), cts.Token);

            Assert.IsFalse(task.IsCanceled);

            cts.Cancel();

            Assert.IsTrue(task.IsCanceled);

            Assert.Greater(Job.CancelCount, 0);
        }

        [Test]
        public void TestJavaTask()
        {
            // TODO
        }

        private class Task : IComputeTask<int, IList<IComputeJobResult<int>>>
        {
            public IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
            {
                Console.WriteLine("Map: " + Thread.CurrentThread.ManagedThreadId);
                return Enumerable.Range(1, 10)
                    .SelectMany(x => subgrid)
                    .ToDictionary(x => (IComputeJob<int>)new Job(), x => x);
            }

            public ComputeJobResultPolicy OnResult(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
            {
                Console.WriteLine("Result: " + Thread.CurrentThread.ManagedThreadId);
                return ComputeJobResultPolicy.Wait;
            }

            public IList<IComputeJobResult<int>> Reduce(IList<IComputeJobResult<int>> results)
            {
                return results;
            }
        }

        [Serializable]
        private class Job : IComputeJob<int>
        {
            private static int _cancelCount = 0;

            public static int CancelCount
            {
                get { return Thread.VolatileRead(ref _cancelCount); }
                set { Thread.VolatileWrite(ref _cancelCount, value); }
            }

            public int Execute()
            {
                Thread.Sleep(20);
                Console.WriteLine("Execute: " + Thread.CurrentThread.ManagedThreadId);
                return 1;
            }

            public void Cancel()
            {
                Interlocked.Increment(ref _cancelCount);
            }
        }

    }
}
