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
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;
    using static TestUtils;

    /// <summary>
    /// Tests for <see cref="IComputeTaskSession"/>
    /// </summary>
    public class ComputeTaskNameTest : TestBase
    {
        /// <summary>
        /// .Net compute task name is propagated into platform task.
        /// </summary>
        [Test]
        public void TaskNameTakenFromPlatformTask()
        {
            // Call task asynchronously with delay
            var task = new LongTask(3000);
            var cts = new CancellationTokenSource();
            Ignite.GetCompute().ExecuteAsync(task, 123, cts.Token);

            try
            {
                // Check task in system views via SQL
                var res = Ignite
                    .GetOrCreateCache<string, string>("test")
                    .Query(new SqlFieldsQuery("SELECT TASK_NAME, TASK_CLASS_NAME FROM SYS.TASKS", null))
                    .GetAll()
                    .Single();

                Assert.AreEqual("Apache.Ignite.Core.Tests.Compute.ComputeTaskNameTest+LongTask", res[0]);
                Assert.AreEqual("org.apache.ignite.internal.processors.platform.compute.PlatformFullTask", res[1]);
            }
            finally
            {
                cts.Cancel();
            }
        }

        /// <summary>
        /// .Net compute closure name is propagated into platform task.
        /// </summary>
        [Test]
        public void ClosureNameTakenFromPlatformTask()
        {
            // Call task asynchronously with delay
            var clo = new LongClosure(3000);
            var cts = new CancellationTokenSource();
            Ignite.GetCompute().CallAsync(clo, cts.Token);

            try
            {
                // Check task in system views via SQL
                var res = Ignite
                    .GetOrCreateCache<string, string>("test")
                    .Query(new SqlFieldsQuery("SELECT TASK_NAME, TASK_CLASS_NAME FROM SYS.TASKS", null))
                    .GetAll()
                    .Single();
                
                Assert.AreEqual("Apache.Ignite.Core.Tests.Compute.LongClosure", res[0]);
                Assert.AreEqual("org.apache.ignite.internal.processors.platform.compute.PlatformBalancingSingleClosureTask", res[1]);
            }
            finally
            {
                cts.Cancel();
            }
        }

        /// <summary>
        /// .Net broadcast compute closure name is propagated into platform task.
        /// </summary>
        [Test]
        public void BroadcastClosureNameTakenFromPlatformTask()
        {
            // Call task asynchronously with delay
            var clo = new LongClosure(3000);
            var cts = new CancellationTokenSource();
            Ignite.GetCompute().BroadcastAsync(clo, cts.Token);

            try
            {
                // Check task in system views via SQL
                var res = Ignite
                    .GetOrCreateCache<string, string>("test")
                    .Query(new SqlFieldsQuery("SELECT TASK_NAME, TASK_CLASS_NAME FROM SYS.TASKS", null))
                    .GetAll()
                    .Single();
                
                Assert.AreEqual("Apache.Ignite.Core.Tests.Compute.LongClosure", res[0]);
                Assert.AreEqual("org.apache.ignite.internal.processors.platform.compute.PlatformBroadcastingSingleClosureTask", res[1]);
            }
            finally
            {
                cts.Cancel();
            }
        }

        /// <summary>
        /// Creates a task that executes <see cref="LongJob"/>.
        /// </summary>
        [ComputeTaskSessionFullSupport]
        private class LongTask : ComputeTaskSplitAdapter<int, string, string>
        {
            /// Delay time in milliseconds.
            private readonly int _delay;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="delay">Execution delay time in milliseconds</param>
            public LongTask(int delay)
            {
                _delay = delay;
            }

            /// <inheritdoc />
            public override string Reduce(IList<IComputeJobResult<string>> results) => results[0].Data;

            /// <inheritdoc />
            protected override ICollection<IComputeJob<string>> Split(int gridSize, int attrValue)
            {
                return new List<IComputeJob<string>> { new LongJob(_delay) };
            }
        }

        /// <summary>
        /// Implements delayed job execution.
        /// </summary>
        private class LongJob : ComputeJobAdapter<string>
        {
            /// Delay time in milliseconds.
            private readonly int _delay;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="delay">Execution delay time in milliseconds</param>
            public LongJob(int delay)
            {
                _delay = delay;
            }

            /// <inheritdoc />
            public override string Execute()
            {
                Thread.Sleep(_delay);
                
                return "OK";   
            }
        }
    }
    
    /// <summary>
    /// Implements closure with delayed execution.
    /// </summary>
    [Serializable]
    public class LongClosure : IComputeFunc<String>
    {
        /// Delay time in milliseconds.
        private readonly int _delay;

        /// <summary>
        ///
        /// </summary>
        /// <param name="s"></param>
        public LongClosure(int delay)
        {
            _delay = delay;
        }

        /** <inheritDoc /> */
        public string Invoke()
        {
            Thread.Sleep(_delay);
                
            return "OK";   
        }
    }
}
