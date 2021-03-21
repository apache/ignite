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
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests compute async continuation behavior.
    /// </summary>
    public class ComputeTestAsyncAwait : TestBase
    {
        /// <summary>
        /// TODO
        /// </summary>
        [Test]
        public async Task TestComputeRunAsyncContinuation()
        {
            // TODO: Test local and remote execution - where do we end up?
            // Test Tasks and Funcs.
            var compute = Ignite.GetCompute();

            // TODO: Most of the Compute goes through PlatformAbstractTask.reduce, except for Affinity* overloads
            // - test them separately.
            await compute.RunAsync(new ComputeAction());

            // TODO: More problems: here we hold the pub- thread that called PlatformAbstractTask.reduce,
            // so reducing never completes and there is a resource leak.
            StringAssert.StartsWith("ForkJoinPool.commonPool-worker-", TestUtilsJni.GetJavaThreadName());
        }

        /// <summary>
        /// TODO
        /// </summary>
        [Test]
        public async Task TestComputeExecuteAsyncContinuation()
        {
            var compute = Ignite.GetCompute();

            await compute.ExecuteAsync(new StringLengthEmptyTask(), "abc");

            StringAssert.StartsWith("ForkJoinPool.commonPool-worker-", TestUtilsJni.GetJavaThreadName());
        }

        /// <summary>
        /// TODO
        /// </summary>
        [Test]
        public async Task TestComputeAffinityRunAsyncContinuation()
        {
            // TODO: Test local and remote execution.
            var compute = Ignite.GetCompute();
            var cache = Ignite.GetOrCreateCache<int, int>("c");

            await compute.AffinityRunAsync(new[] {cache.Name}, 1, new ComputeAction());

            StringAssert.StartsWith("ForkJoinPool.commonPool-worker-", TestUtilsJni.GetJavaThreadName());
        }
    }
}
