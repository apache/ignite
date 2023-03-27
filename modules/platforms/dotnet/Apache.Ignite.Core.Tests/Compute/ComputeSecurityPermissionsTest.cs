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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using NUnit.Framework;
    using static AbstractTask.Job;
    using static ComputeSecurityPermissionsTest;

    /// <summary>
    /// Tests authorization of DotNet native Compute tasks execution.
    /// </summary> 
    public class ComputeSecurityPermissionsTest
    {
        private const string CacheName = "DEFAULT_CACHE_NAME";
        
        public static int ExecutedJobCounter;
        
        public static int CancelledJobCounter;
        
        private IIgnite _grid;

        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = @"Config/Compute/compute-security.xml",
            };

            _grid = Ignition.Start(cfg);

            _grid.CreateCache<object, object>(CacheName);
        }

        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public void TestComputeSecurityExecutePermission()
        {
            CheckTask((task, ct) => _grid.GetCompute().Execute(task, null));
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync(task, null).GetResult());
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync(task, null, ct).GetResult());

            CheckTask((task, ct) => _grid.GetCompute().Execute(task));
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync(task).GetResult());
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync(task, ct).GetResult());

            CheckTask((task, ct) => _grid.GetCompute().Execute<object, int, int>(task.GetType(), null));
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync<object, int, int>(task.GetType(), null).GetResult());
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync<object, int, int>(task.GetType(), null, ct).GetResult());

            CheckTask((task, ct) => _grid.GetCompute().Execute<int, int>(task.GetType()));
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync<int, int>(task.GetType()).GetResult());
            CheckTask((task, ct) => _grid.GetCompute().ExecuteAsync<int, int>(task.GetType(), ct).GetResult());

            CheckCallable((func, ct) => _grid.GetCompute().Call(func));
            CheckCallable((func, ct) => _grid.GetCompute().CallAsync(func).GetResult());
            CheckCallable((func, ct) => _grid.GetCompute().CallAsync(func, ct).GetResult());

            CheckCallable((func, ct) => _grid.GetCompute().AffinityCall(CacheName, 0, func));
            CheckCallable((func, ct) => _grid.GetCompute().AffinityCallAsync(CacheName, 0, func).GetResult());
            CheckCallable((func, ct) => _grid.GetCompute().AffinityCallAsync(CacheName, 0, func, ct).GetResult());

            CheckCallable((func, ct) => _grid.GetCompute().AffinityCall(new[] { CacheName }, 0, func));
            CheckCallable((func, ct) => _grid.GetCompute().AffinityCallAsync(new[] { CacheName }, 0, func).GetResult());
            CheckCallable((func, ct) => _grid.GetCompute().AffinityCallAsync(new[] { CacheName }, 0, func, ct).GetResult());

            CheckCallables((callables, ct) => _grid.GetCompute().Call(callables, new TestReducer()));
            CheckCallables((callables, ct) => _grid.GetCompute().CallAsync(callables, new TestReducer()).GetResult());
            CheckCallables((callables, ct) => _grid.GetCompute().CallAsync(callables, new TestReducer(), ct).GetResult());

            CheckCallables((callables, ct) => _grid.GetCompute().Call(callables));
            CheckCallables((callables, ct) => _grid.GetCompute().CallAsync(callables).GetResult());
            CheckCallables((callables, ct) => _grid.GetCompute().CallAsync(callables, ct).GetResult());

            CheckCallable((callable, ct) => _grid.GetCompute().Broadcast(callable));
            CheckCallable((callable, ct) => _grid.GetCompute().BroadcastAsync(callable).GetResult());
            CheckCallable((callable, ct) => _grid.GetCompute().BroadcastAsync(callable, ct).GetResult());

            CheckFunction((func, ct) => _grid.GetCompute().Broadcast(func, 0));
            CheckFunction((func, ct) => _grid.GetCompute().BroadcastAsync(func, 0).GetResult());
            CheckFunction((func, ct) => _grid.GetCompute().BroadcastAsync(func, 0, ct).GetResult());

            CheckAction((action, ct) => _grid.GetCompute().Broadcast(action));
            CheckAction((action, ct) => _grid.GetCompute().BroadcastAsync(action).Wait());
            CheckAction((action, ct) => _grid.GetCompute().BroadcastAsync(action, ct).Wait());

            CheckAction((action, ct) => _grid.GetCompute().Run(action));
            CheckAction((action, ct) => _grid.GetCompute().RunAsync(action).Wait());
            CheckAction((action, ct) => _grid.GetCompute().RunAsync(action, ct).Wait());

            CheckAction((action, ct) => _grid.GetCompute().AffinityRun(CacheName, 0, action));
            CheckAction((action, ct) => _grid.GetCompute().AffinityRunAsync(CacheName, 0, action).Wait());
            CheckAction((action, ct) => _grid.GetCompute().AffinityRunAsync(CacheName, 0, action, ct).Wait());

            CheckAction((action, ct) => _grid.GetCompute().AffinityRun(new[] { CacheName }, 0, action));
            CheckAction((action, ct) => _grid.GetCompute().AffinityRunAsync(new[] { CacheName }, 0, action).Wait());
            CheckAction((action, ct) => _grid.GetCompute().AffinityRunAsync(new[] { CacheName }, 0, action, ct).Wait());

            CheckActions((actions, ct) => _grid.GetCompute().Run(actions));
            CheckActions((actions, ct) => _grid.GetCompute().RunAsync(actions).Wait());
            CheckActions((actions, ct) => _grid.GetCompute().RunAsync(actions, ct).Wait());

            CheckFunction((func, ct) => _grid.GetCompute().Apply(func, 0));
            CheckFunction((func, ct) => _grid.GetCompute().ApplyAsync(func, 0).GetResult());
            CheckFunction((func, ct) => _grid.GetCompute().ApplyAsync(func, 0, ct).GetResult());

            CheckFunction((func, ct) => _grid.GetCompute().Apply(func, new[] { 0 }));
            CheckFunction((func, ct) => _grid.GetCompute().ApplyAsync(func, new[] { 0 }).GetResult());
            CheckFunction((func, ct) => _grid.GetCompute().ApplyAsync(func, new[] { 0 }, ct).GetResult());

            CheckFunction((func, ct) => _grid.GetCompute().Apply(func, new[] { 0 }, new TestReducer()));
            CheckFunction((func, ct) => _grid.GetCompute().ApplyAsync(func, new[] { 0 }, new TestReducer()).GetResult());
            CheckFunction((func, ct) => _grid.GetCompute().ApplyAsync(func, new[] { 0 }, new TestReducer(), ct).GetResult());
        }
        
        [Test]
        public void TestComputeTaskSecurityCancelPermission()
        {
            CheckTaskCancel((task, ct) => _grid.GetCompute().ExecuteAsync(task, null, ct));
            CheckTaskCancel((task, ct) => _grid.GetCompute().ExecuteAsync(task, ct));
            CheckTaskCancel((task, ct) => _grid.GetCompute().ExecuteAsync<object, int, int>(task.GetType(), null, ct));
            CheckTaskCancel((task, ct) => _grid.GetCompute().ExecuteAsync<int, int>(task.GetType(), ct));
        }

        private void CheckFunction(Action<IComputeFunc<int, int>, CancellationToken> executor)
        {
            CheckExecutionSucceeded(token => executor(new ExecuteAllowedFunction(), token));
            CheckExecutionFailed(token => executor(new ExecuteForbiddenFunction(), token));
        }

        private void CheckCallable(Action<IComputeFunc<int>, CancellationToken> executor)
        {
            CheckExecutionSucceeded(token => executor(new ExecuteAllowedCallable(), token));
            CheckExecutionFailed(token => executor(new ExecuteForbiddenCallable(), token));
        }

        private void CheckCallables(Action<IEnumerable<IComputeFunc<int>>, CancellationToken> executor)
        {
            CheckExecutionSucceeded(token => executor(new[] { new ExecuteAllowedCallable() }, token));
            CheckExecutionFailed(token =>
                executor(new IComputeFunc<int>[] { new ExecuteAllowedCallable(), new ExecuteForbiddenCallable() }, token));
        }

        private void CheckAction(Action<IComputeAction, CancellationToken> executor)
        {
            CheckExecutionSucceeded(token => executor(new ExecuteAllowedAction(), token));
            CheckExecutionFailed(token => executor(new ExecuteForbiddenAction(), token));
        }

        private void CheckActions(Action<IEnumerable<IComputeAction>, CancellationToken> executor)
        {
            CheckExecutionSucceeded(token => executor(new[] { new ExecuteAllowedAction() }, token));
            CheckExecutionFailed(token =>
                executor(new IComputeAction[] { new ExecuteAllowedAction(), new ExecuteForbiddenAction() }, token));
        }

        private void CheckTask(Action<IComputeTask<int, int>, CancellationToken> executor)
        {
            CheckExecutionSucceeded(token => executor(new ExecuteAllowedTask(), token));
            CheckExecutionFailed(token => executor(new ExecuteForbiddenTask(), token));
        }
        
        private void CheckTaskCancel(Func<IComputeTask<int, int>, CancellationToken, Task<int>> executor)
        {
            CheckTaskCancelSucceeded(executor);
            CheckTaskCancelFailed(executor);
        }
        
        private void CheckTaskCancelFailed(Func<IComputeTask<int, int>, CancellationToken, Task<int>> executor)
        {
           CancelledJobCounter = 0;
           ExecutedJobCounter = 0;
           
           JobStartedLatch = new CountdownEvent(1);
           JobUnblockedLatch = new CountdownEvent(1);

           using var cts = new CancellationTokenSource();

           var fut = executor.Invoke(new ExecuteAllowedTask(), cts.Token);

           JobStartedLatch.Wait(5000);

           AssertAuthorizationException(() => cts.Cancel());
           
           Assert.False(fut.IsCanceled);
           
           JobUnblockedLatch.Signal();
           
           Assert.AreEqual(0, CancelledJobCounter);
           TestUtils.WaitForTrueCondition(() => 1 == ExecutedJobCounter); 
        }

        private void CheckTaskCancelSucceeded(Func<IComputeTask<int, int>, CancellationToken, Task<int>> executor)
        {
            CancelledJobCounter = 0;
            ExecutedJobCounter = 0;
            
            JobStartedLatch = new CountdownEvent(1);
            JobUnblockedLatch = new CountdownEvent(1);

            using var cts = new CancellationTokenSource();

            var fut = executor.Invoke(new ExecuteCancelAllowedTask(), cts.Token);

            JobStartedLatch.Wait(5000);

            cts.Cancel();
            
            TestUtils.WaitForTrueCondition(() => fut.IsCanceled);
            
            JobUnblockedLatch.Signal();
            
            Assert.AreEqual(1, CancelledJobCounter);
            Assert.AreEqual(0, ExecutedJobCounter);
        }

        private void CheckExecutionSucceeded(Action<CancellationToken> action)
        {
            ExecutedJobCounter = 0;

            using var cts = new CancellationTokenSource();

            action(cts.Token);

            Assert.AreEqual(1, ExecutedJobCounter);
        }

        private void CheckExecutionFailed(Action<CancellationToken> action)
        {
            ExecutedJobCounter = 0;

            using var cts = new CancellationTokenSource();
            
            var token = cts.Token;
            
            AssertAuthorizationException(() => action(token));

            Assert.AreEqual(0, ExecutedJobCounter);
        }

        private void AssertAuthorizationException(TestDelegate action)
        {
            var ex = Assert.Catch(action) ;

            Assert.NotNull(ex);
            StringAssert.Contains("Authorization failed", ex.GetBaseException().Message);
        }
    }

    public class TestReducer : IComputeReducer<int, object>
    {
        /** <inheritdoc /> */
        public bool Collect(int res)
        {
            return true;
        }

        /** <inheritdoc /> */
        public object Reduce()
        {
            return null;
        }
    }

    public class ExecuteAllowedAction : AbstractAction { }

    public class ExecuteForbiddenAction : AbstractAction { }

    public abstract class AbstractAction : IComputeAction
    {
        /** <inheritDoc /> */
        public void Invoke()
        {
           Interlocked.Increment(ref ExecutedJobCounter); 
        }
    }

    public class ExecuteAllowedFunction : AbstractFunction { }

    public class ExecuteForbiddenFunction : AbstractFunction { }

    public abstract class AbstractFunction : IComputeFunc<int, int>
    {
        /** <inheritDoc /> */
        public int Invoke(int arg)
        {
            Interlocked.Increment(ref ExecutedJobCounter);
            
            return 42;
        }
    }

    public class ExecuteAllowedCallable : AbstractCallable { }

    public class ExecuteForbiddenCallable : AbstractCallable { }

    public abstract class AbstractCallable : IComputeFunc<int>
    {
        public int Invoke()
        {
            Interlocked.Increment(ref ExecutedJobCounter);
            
            return 42;
        }
    }

    public class ExecuteAllowedTask : AbstractTask { }
    
    public class ExecuteCancelAllowedTask : AbstractTask { }

    public class ExecuteForbiddenTask : AbstractTask { }

    [Serializable]
    public abstract class AbstractTask : IComputeTask<int, int>
    {
        public IDictionary<IComputeJob<int>, IClusterNode> Map(IList<IClusterNode> subgrid, object arg)
        {
            return subgrid.ToDictionary(x => (IComputeJob<int>)new Job(), x => x);
        }

        public ComputeJobResultPolicy OnResult(IComputeJobResult<int> res, IList<IComputeJobResult<int>> rcvd)
        {
            return ComputeJobResultPolicy.Wait;
        }

        public int Reduce(IList<IComputeJobResult<int>> results)
        {
            foreach (var res in results)
            {
                Exception err = res.Exception;

                if (err != null)
                {
                    throw err;
                }
            }

            return 42;
        }

        [Serializable]
        public class Job : IComputeJob<int>
        {
            public static CountdownEvent JobStartedLatch;
        
            public static CountdownEvent JobUnblockedLatch;
            
            private bool isCancelled;
            
            /** <inheritdoc /> */
            public int Execute()
            {
               JobStartedLatch?.Signal();
               JobUnblockedLatch?.Wait(5000);
               
               if (!isCancelled)
                   Interlocked.Increment(ref ExecutedJobCounter);
                           
               return 42; 
            }

            /** <inheritdoc /> */
            public void Cancel()
            {
                isCancelled = true;
                
                Interlocked.Increment(ref CancelledJobCounter);
            }
        }
    }
}