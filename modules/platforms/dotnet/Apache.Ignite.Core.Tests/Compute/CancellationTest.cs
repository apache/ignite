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
    public class CancellationTest : SpringTestBase
    {
        /** */
        private const int MillisecondsTimeout = 50;

        public CancellationTest()
            : base("Config/Compute/compute-grid1.xml", "Config/Compute/compute-grid2.xml")
        {
            // No-op.
        }

        [Test]
        public void TestTask()
        {
            TestTask((c, t) => c.ExecuteAsync(new Task(), t));
        }

        [Test]
        public void TestTaskWithArg()
        {
            TestTask((c, t) => c.ExecuteAsync(new Task(), 1, t));
        }

        [Test]
        public void TestTaskByType()
        {
            TestTask((c, t) => c.ExecuteAsync<int, IList<IComputeJobResult<int>>>(typeof(Task), t));
        }

        [Test]
        public void TestTaskByTypeWithArg()
        {
            TestTask((c, t) => c.ExecuteAsync<object, int, IList<IComputeJobResult<int>>>(typeof(Task), 1, t));
        }

        [Test]
        public void TestJavaTask()
        {
            using (var cts = new CancellationTokenSource())
            {
                var task = Compute.ExecuteJavaTaskAsync<object>(ComputeApiTest.BroadcastTask, null, cts.Token);

                Assert.IsFalse(task.IsCanceled);

                cts.Cancel();

                Assert.IsTrue(task.IsCanceled);

                // Pass cancelled token
                Assert.IsTrue(
                    Compute.ExecuteJavaTaskAsync<object>(ComputeApiTest.BroadcastTask, null, cts.Token).IsCanceled);
            }
        }

        [Test]
        public void TestClosures()
        {
            TestClosure((c, t) => c.BroadcastAsync(new ComputeAction(), t));
            TestClosure((c, t) => c.BroadcastAsync(new ComputeFunc(), t));
            TestClosure((c, t) => c.BroadcastAsync(new ComputeBiFunc(), 10, t));

            TestClosure((c, t) => c.AffinityRunAsync("default", 0, new ComputeAction(), t));

            TestClosure((c, t) => c.RunAsync(new ComputeAction(), t));
            TestClosure((c, t) => c.RunAsync(Enumerable.Range(1, 10).Select(x => new ComputeAction()), t));

            TestClosure((c, t) => c.CallAsync(new ComputeFunc(), t));
            TestClosure((c, t) => c.CallAsync(Enumerable.Range(1, 10).Select(x => new ComputeFunc()), t));
            TestClosure((c, t) => c.CallAsync(Enumerable.Range(1, 10).Select(x => new ComputeFunc()),
                new ComputeReducer(), t));

            TestClosure((c, t) => c.AffinityCallAsync("default", 0, new ComputeFunc(), t));

            TestClosure((c, t) => c.ApplyAsync(new ComputeBiFunc(), 10, t));
            TestClosure((c, t) => c.ApplyAsync(new ComputeBiFunc(), Enumerable.Range(1, 100), t));
            TestClosure((c, t) => c.ApplyAsync(new ComputeBiFunc(), Enumerable.Range(1, 100), new ComputeReducer(), t));
        }

        private void TestTask(Func<ICompute, CancellationToken, System.Threading.Tasks.Task> runner)
        {
            Job.Cancelled = false;
            Job.StartEvent.Reset();
            Job.CancelEvent.Reset();

            using (var cts = new CancellationTokenSource())
            {
                var task = runner(Compute, cts.Token);
                Assert.IsFalse(task.IsCanceled);

                Job.StartEvent.Wait();

                cts.Cancel();
                TestUtils.WaitForTrueCondition(() => task.IsCanceled);

                // Pass cancelled token
                Assert.IsTrue(runner(Compute, cts.Token).IsCanceled);
            }

            Assert.IsTrue(TestUtils.WaitForCondition(() => Job.Cancelled, 5000));
        }

        private void TestClosure(Func<ICompute, CancellationToken, System.Threading.Tasks.Task> runner, int delay = 0)
        {
            using (var cts = new CancellationTokenSource())
            {
                var task = runner(Compute, cts.Token);

                Thread.Sleep(delay);

                Assert.IsFalse(task.IsCanceled);

                cts.Cancel();

                TestUtils.WaitForTrueCondition(() => task.IsCanceled);

                // Pass cancelled token
                Assert.IsTrue(runner(Compute, cts.Token).IsCanceled);
            }
        }

        private class Task : IComputeTask<int, IList<IComputeJobResult<int>>>
        {
            public IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
            {
                return subgrid.Where(x => !x.IsLocal).Take(1).ToDictionary(x => (IComputeJob<int>)new Job(), x => x);
            }

            public ComputeJobResultPolicy OnResult(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
            {
                return ComputeJobResultPolicy.Wait;
            }

            public IList<IComputeJobResult<int>> Reduce(IList<IComputeJobResult<int>> results)
            {
                Assert.Fail("Reduce should not be called on a cancelled task.");
                return results;
            }
        }

        [Serializable]
        private class Job : IComputeJob<int>
        {
            public static readonly ManualResetEventSlim StartEvent = new ManualResetEventSlim(false);

            public static readonly ManualResetEventSlim CancelEvent = new ManualResetEventSlim(false);

            public static volatile bool Cancelled;

            public int Execute()
            {
                StartEvent.Set();
                CancelEvent.Wait();

                return 1;
            }

            public void Cancel()
            {
                CancelEvent.Set();
                Cancelled = true;
            }
        }

        [Serializable]
        private class ComputeBiFunc : IComputeFunc<int, int>
        {
            public int Invoke(int arg)
            {
                Thread.Sleep(MillisecondsTimeout);
                return arg;
            }
        }

        private class ComputeReducer : IComputeReducer<int, int>
        {
            public bool Collect(int res)
            {
                return true;
            }

            public int Reduce()
            {
                return 0;
            }
        }
    }
}
